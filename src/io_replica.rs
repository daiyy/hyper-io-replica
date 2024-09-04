use io_uring::{opcode, squeue, types};
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{ctrl::UblkCtrl, helpers::IoBuf, UblkError, UblkIORes};
use libublk::sys::ublksrv_io_desc;
use libublk::uring_async::ublk_wake_task;
use log::{trace, debug};
use serde::{Deserialize, Serialize};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Duration, Instant};
use std::collections::HashSet;
use bytesize::ByteSize;
use crate::pool::{PendingIo, LocalPendingBlocksPool, TgtPendingBlocksPool};
use crate::state::{LocalTgtState, GlobalTgtState};
use crate::state;
use crate::region;
use crate::recover;

#[derive(clap::Args, Debug)]
pub struct IoReplicaArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,

    /// backing file of ublk target
    #[clap(long, short = 'f')]
    pub file: PathBuf,

    /// buffered io is applied for backing file of ublk target, default is direct IO
    #[clap(long, default_value_t = false)]
    pub buffered_io: bool,

    /// use async_await
    #[clap(long, short = 'a', default_value_t = false)]
    pub async_await: bool,

    /// replica device
    #[clap(long)]
    pub replica: String,

    /// region size
    #[clap(long, default_value = "8MiB")]
    pub region_size: String,

    /// pool size
    #[clap(long, default_value = "1GiB")]
    pub pool_size: String,

    /// device size, default is entire primary space
    #[clap(long)]
    pub device_size: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
    async_await: bool,
    replica_dev_path: String,
    region_size: u64,
    pool_size: u64,
    device_size: u64,
}

pub(crate) struct LoopTgt {
    pub back_file_path: String,
    pub back_file: std::fs::File,
    pub direct_io: i32,
    pub async_await: bool,
    pub replica_dev_path: String,
    pub region_size: u64,
    pub pool_size: u64,
    pub device_size: u64,
}

std::thread_local! {
    static PENDING_BLOCKS: RefCell<LocalPendingBlocksPool> = panic!("local pending blocks pool is not yet init");
}

std::thread_local! {
    pub(crate) static LOCAL_STATE: RefCell<LocalTgtState> = panic!("local target state is not yet init");
}

std::thread_local! {
    pub(crate) static LOCAL_DIRTY_REGION: RefCell<HashSet<u64>> = panic!("local dirty region set is not yet init");
}

std::thread_local! {
    pub(crate) static LOCAL_REGION_MAP: RefCell<region::Region> = panic!("local incar of global region map is not yet init");
}

std::thread_local! {
    pub(crate) static LOCAL_RECOVER_CTRL: RefCell<recover::RecoverCtrl> = panic!("local incar of global recover ctrl is not yet init");
}

std::thread_local! {
    pub(crate) static LOCAL_REGION_SHIFT: RefCell<u32> = panic!("local region shift value is not yet init");
}

#[inline]
fn pool_append_pending(iod: &ublksrv_io_desc, force_propgate: bool) {
    PENDING_BLOCKS.with(|pool| {

        // copy to pio and append to local pool
        let pio = PendingIo::from_iodesc(&iod);
        pool.borrow_mut().append(pio);

        // if we have avail mem in local pool
        if pool.borrow().avail_capacity() >= (iod.nr_sectors << 9) as usize {
            if force_propgate {
                pool.borrow_mut().propagate();
            }
            return;
        }
        // if no enough avail mem, force propagate
        pool.borrow_mut().propagate();
    })
}

#[inline]
fn pool_get_count() -> usize {
    PENDING_BLOCKS.with(|pool| {
        pool.borrow().get_count()
    })
}

#[inline]
fn pool_propagate() {
    PENDING_BLOCKS.with(|pool| {
        pool.borrow_mut().propagate()
    })
}

#[inline]
fn __lo_prep_submit_io_cmd(iod: &libublk::sys::ublksrv_io_desc) -> i32 {
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH
        | libublk::sys::UBLK_IO_OP_READ
        | libublk::sys::UBLK_IO_OP_WRITE => 0,
        _ => -libc::EINVAL,
    }
}

#[inline]
fn __lo_make_io_sqe(op: u32, off: u64, bytes: u32, buf_addr: *mut u8) -> io_uring::squeue::Entry {
    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::SyncFileRange::new(types::Fixed(1), bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_READ => opcode::Read::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_WRITE => opcode::Write::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        _ => panic!(),
    }
}

#[inline]
async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16, buf_addr: *mut u8) -> i32 {
    let iod = q.get_iod(tag);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    if state::local_state_recovery_forward_part()
            || state::local_state_recovery_forward_full()
            || state::local_state_recovery_reverse_full()
    {
        let op = iod.op_flags & 0xff;
        match op {
            libublk::sys::UBLK_IO_OP_READ => {
                recover::local_recover_ctrl_read(&iod).await;
            },
            libublk::sys::UBLK_IO_OP_WRITE |
            libublk::sys::UBLK_IO_OP_WRITE_SAME |
            libublk::sys::UBLK_IO_OP_DISCARD |
            libublk::sys::UBLK_IO_OP_WRITE_ZEROES |
            libublk::sys::UBLK_IO_OP_FLUSH => {
                recover::local_recover_ctrl_write(&iod).await;
            },
            _ => {},
        }
    }

    for _ in 0..4 {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = iod.start_sector << 9;
        let bytes = iod.nr_sectors << 9;

        // TODO:
        // since back storage is always "write through"
        // we *DO NOT* need to forward FLUSH op to primary (will get -EINVAL if we do this).
        // in the future we need to get write cache setting from primary before make decision
        // if or not to forward FLUSH op here.
        if op == libublk::sys::UBLK_IO_OP_FLUSH {
            // if flush op and logging enabled, log this io and return success immediately
            if state::local_state_logging_enabled() {
                pool_append_pending(&iod, true);
                return 0;
            }
        }

        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr);
        let res = q.ublk_submit_sqe(sqe).await;
        if res != -(libc::EAGAIN) {
            match op {
                libublk::sys::UBLK_IO_OP_WRITE |
                libublk::sys::UBLK_IO_OP_WRITE_SAME |
                libublk::sys::UBLK_IO_OP_DISCARD |
                libublk::sys::UBLK_IO_OP_WRITE_ZEROES => {
                    if state::local_state_logging_enabled() {
                        pool_append_pending(&iod, false);
                    } else {
                        region::local_region_mark_dirty(&iod);
                    }
                },
                libublk::sys::UBLK_IO_OP_FLUSH => {
                    if state::local_state_logging_enabled() {
                        pool_append_pending(&iod, true);
                    }
                },
                _ => {
                },
            }
            return res;
        }
    }

    -libc::EAGAIN
}

fn lo_init_tgt(
    dev: &mut UblkDev,
    lo: &LoopTgt,
    opt: Option<IoReplicaArgs>,
    dio: bool,
) -> Result<(), UblkError> {
    trace!("loop: init_tgt {}", dev.dev_info.dev_id);
    let info = dev.dev_info;

    if lo.direct_io != 0 {
        unsafe {
            libc::fcntl(lo.back_file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
        }
    }

    let tgt = &mut dev.tgt;
    let nr_fds = tgt.nr_fds;
    tgt.fds[nr_fds as usize] = lo.back_file.as_raw_fd();
    tgt.nr_fds = nr_fds + 1;

    let sz = crate::ublk_file_size(&lo.back_file).unwrap();
    let attrs = if dio {
        0
    } else {
        libublk::sys::UBLK_ATTR_VOLATILE_CACHE
    };

    // use device from tgt instead from back file
    tgt.dev_size = lo.device_size;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            attrs,
            logical_bs_shift: sz.1,
            physical_bs_shift: sz.2,
            io_opt_shift: sz.2,
            io_min_shift: sz.1,
            max_sectors: info.max_io_buf_bytes >> 9,
            dev_sectors: tgt.dev_size >> 9,
            chunk_sectors: lo.region_size as u32 >> 9,
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(o) = opt {
        o.gen_arg.apply_block_size(dev);
        o.gen_arg.apply_read_only(dev);
    }

    let val = serde_json::json!({"loop": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: lo.direct_io, async_await:lo.async_await, replica_dev_path: lo.replica_dev_path.clone(), region_size: lo.region_size, pool_size: lo.pool_size, device_size: lo.device_size, } });
    dev.set_target_json(val);

    Ok(())
}

fn to_absolute_path(p: PathBuf, parent: Option<PathBuf>) -> PathBuf {
    if p.is_absolute() {
        p
    } else {
        match parent {
            None => p,
            Some(n) => n.join(p),
        }
    }
}

fn lo_handle_io_cmd_sync(q: &UblkQueue<'_>, tag: u16, i: &UblkIOCtx, buf_addr: *mut u8) {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let data = UblkIOCtx::build_user_data(tag, op, 0, true);
    if i.is_tgt_io() {
        let user_data = i.user_data();
        let res = i.result();
        let cqe_tag = UblkIOCtx::user_data_to_tag(user_data);
        let op = UblkIOCtx::user_data_to_op(user_data);
        debug!("<- target io op: {}, res: {}, tag: {}, qid: {}, tid: {:?}",
            op, res, cqe_tag, q.get_qid(), std::thread::current().id());

        assert!(cqe_tag == tag as u32);

        if res != -(libc::EAGAIN) {
            if op >= 1 && op <= 5 {
                //UBLK_IO_OP_WRITE | UBLK_IO_OP_WRITE_SAME | UBLK_IO_OP_FLUSH | UBLK_IO_OP_DISCARD | UBLK_IO_OP_WRITE_ZEROES ->
                pool_append_pending(&iod, true);
            }
            q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
            return;
        }
    }

    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        debug!("-- complete io due to res: {}", res);
        q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
    } else {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = iod.start_sector << 9;
        let bytes = iod.nr_sectors << 9;
        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr).user_data(data);
        q.ublk_submit_sqe_sync(sqe).unwrap();
        debug!("-> submit target io op: {}, off: {}, bytes: {}, ptr: {:?}", op, off, bytes, buf_addr);
        debug!("   io desc: {:?}", iod);
    }
}

fn new_q_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..depth {
        let q = q_rc.clone();

        f_vec.push(exe.spawn(async move {
            let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
            let buf_addr = buf.as_mut_ptr();
            let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
            let mut res = 0;

            q.register_io_buf(tag, &buf);
            loop {
                let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                    break;
                }

                res = lo_handle_io_cmd_async(&q, tag, buf_addr).await;
                cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
            }
        }));
    }

    f_vec.push(exe.spawn(async move {
        // task to propagate thread local cache once per second
        loop {
            smol::Timer::after(Duration::from_secs(1)).await;
            let pool_count = pool_get_count();
            if pool_count == 0 {
                continue;
            }
            pool_propagate();
        }
    }));

    // tracking wait time of flush_and_wake_io_tasks
    let mut wait_time = Duration::new(0, 0);
    // expand code of ublk_wait_and_handle_ios(&exe, &q_rc);
    // this is main loop of queue
    loop {
        // let check state change before any real activity happen
        let changed = state::local_state_sync(&wait_time);
        // NOTICE:
        // when transition from logging enable/disable -> recovery mode
        // check if we still have dirty region in local hash set with not yet report to global
        if changed && state::local_state_recovery() {
            if region::local_region_dirty_count() > 0 {
                panic!("qid({}) local state changed, \
                    but local dirty region set is still have {} regions not yet report to global",
                    qid, region::local_region_dirty_count()
                );
            }
        }

        while exe.try_tick() {}

        let mut to_wait = if pool_get_count() > 0 {
            // local pool is not empty, let's keep on executor tick
            0
        } else {
            // local pool is empty, so we set to_wait to 1 for blocking
            // currently max wait time is drive by UBLK_QUEUE_IDLE_SECS = 20
            1
        };
        region::local_region_map_sync(region::local_region_take());
        // if still have inflight io, let's keep looping
        if q_rc.get_inflight_nr_io() > 0 {
            to_wait = 0;
        }
        let start = Instant::now();
        if q_rc.flush_and_wake_io_tasks(|data, cqe, _| ublk_wake_task(data, cqe), to_wait)
            .is_err()
        {
            break;
        }
        wait_time = start.elapsed();
    }

    // clean up buf
    for tag in 0..q_rc.get_depth() as u16 {
        q_rc.unregister_io_buf(tag);
    }

    smol::block_on(async { futures::future::join_all(f_vec).await });
}

#[allow(dead_code)]
fn q_fn(qid: u16, dev: &UblkDev) {

    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();
    let lo_io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let bufs = bufs_rc.clone();

        lo_handle_io_cmd_sync(q, tag, io, bufs[tag as usize].as_mut_ptr());
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs))
        .wait_and_handle_io(lo_io_handler);
}

fn q_a_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..depth {
        let q = q_rc.clone();

        f_vec.push(exe.spawn(async move {
            let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
            let buf_addr = buf.as_mut_ptr();
            let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
            let mut res = 0;

            q.register_io_buf(tag, &buf);
            loop {
                let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                    break;
                }

                res = lo_handle_io_cmd_async(&q, tag, buf_addr).await;
                cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
            }
        }));
    }
    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub(crate) fn ublk_add_io_replica(ctrl: UblkCtrl, opt: Option<IoReplicaArgs>) -> Result<i32, UblkError> {
    let (file, dio, ro, aa, _shm, fg, replica, region_sz, pool_sz, device_sz) = match opt {
        Some(ref o) => {
            let parent = o.gen_arg.get_start_dir();

            (
                to_absolute_path(o.file.clone(), parent),
                !o.buffered_io,
                o.gen_arg.read_only,
                o.async_await,
                Some(o.gen_arg.get_shm_id()),
                o.gen_arg.foreground,
                o.replica.clone(),
                o.region_size.parse::<ByteSize>().expect("unable to parse input region size").0,
                o.pool_size.parse::<ByteSize>().expect("unable to parse input pool size").0,
                o.device_size.as_ref().map_or(0, |s| s.parse::<ByteSize>().expect("unable to parse input device size").0),
            )
        }
        None => {
            let mut p: libublk::sys::ublk_params = Default::default();
            ctrl.get_params(&mut p)?;

            match ctrl.get_target_data_from_json() {
                Some(val) => {
                    let lo = &val["loop"];
                    let tgt_data: Result<LoJson, _> = serde_json::from_value(lo.clone());

                    match tgt_data {
                        Ok(t) => (
                            PathBuf::from(t.back_file_path.as_str()),
                            t.direct_io != 0,
                            (p.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0,
                            t.async_await,
                            None,
                            false,
                            t.replica_dev_path,
                            t.region_size,
                            t.pool_size,
                            t.device_size,
                        ),
                        Err(_) => return Err(UblkError::OtherError(-libc::EINVAL)),
                    }
                }
                None => return Err(UblkError::OtherError(-libc::EINVAL)),
            }
        }
    };

    let file_path = format!("{}", file.as_path().display());
    let mut lo = LoopTgt {
        back_file: std::fs::OpenOptions::new()
            .read(true)
            .write(!ro)
            .open(&file)
            .unwrap(),
        direct_io: i32::from(dio),
        back_file_path: file_path.clone(),
        async_await: aa,
        replica_dev_path: replica.clone(),
        region_size: region_sz,
        pool_size: pool_sz,
        device_size: device_sz,
    };

    //todo: USER_COPY should be the default option
    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let tgt_state = GlobalTgtState::new();
    let g_state = tgt_state.state_clone();

    let (back_file_size, _, _) = crate::ublk_file_size(&lo.back_file).unwrap();
    if lo.device_size == 0 {
        lo.device_size = back_file_size;
    }
    let g_region = region::Region::new(lo.device_size, region_sz);

    // reset region size with checked one
    lo.region_size = g_region.region_size();

    let g_recover_ctrl = recover::RecoverCtrl::default()
        .with_region_size(g_region.region_size())
        .with_nr_regions(g_region.nr_regions())
        .with_g_state(tgt_state.clone())
        .with_primary_path(&file_path)
        .with_replica_path(&replica);

    let tgt = TgtPendingBlocksPool::new(pool_sz as usize, &replica);
    let tx = tgt.get_tx_chan();
    let main = tgt.start(tgt_state, g_region.clone(), g_recover_ctrl.clone());
    let region_shift = g_region.region_shift();

    ctrl.run_target(
        |dev: &mut UblkDev| lo_init_tgt(dev, &lo, opt, dio),
        move |qid, dev: &_| if aa {
            q_a_fn(qid, dev)
        } else {
            // setup thread local pending blocks pool
            let pool = LocalPendingBlocksPool::new(usize::MAX, tx);
            PENDING_BLOCKS.set(pool);

            // setup thread local state
            let state = LocalTgtState::new(qid, g_state.clone());
            LOCAL_STATE.set(state);

            // setup thread local dirty region set
            let region_set = HashSet::new();
            LOCAL_DIRTY_REGION.set(region_set);

            // setup thread local incar of global region bitmap
            LOCAL_REGION_MAP.set(g_region.clone());

            // setup thread local incar of global recover ctrl
            LOCAL_RECOVER_CTRL.set(g_recover_ctrl.clone());

            // setup thread local region shift value
            LOCAL_REGION_SHIFT.set(region_shift);

            new_q_fn(qid, dev)
        },
        move |ctrl: &UblkCtrl| crate::rublk_prep_dump_dev(_shm, fg, ctrl),
    )
    .unwrap();

    let _ = main.join().expect("Couldn't join main thread");
    Ok(0)
}
