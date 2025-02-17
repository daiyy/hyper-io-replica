use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;
use std::time::SystemTime;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;
use libublk::{ctrl::UblkCtrl, UblkError, UblkFlags};
use smol::channel;
use smol::LocalExecutor;
use log::{info, debug, warn};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::mgmt::CommandChannel;
use crate::replica::Replica;
use crate::device::{MetaDeviceDesc, MetaDevice};

pub(crate) enum PendingIo {
    Write(WriteIo),
    Flush(FlushIo),
}

pub(crate) struct WriteIo {
    flags: u32,
    size: u32,
    offset: u64,
    data: IoBuf<u8>,
}

pub(crate) struct FlushIo {
    flags: u32,
    size: u32,
    offset: u64,
}

impl fmt::Debug for PendingIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(io) => write!(f, "PendingIo {{ op: Write, flags: {}, offset: {}, size: {} }}", io.flags, io.offset, io.size),
            Self::Flush(io) => write!(f, "PendingIo {{ op: Flush, flags: {}, offset: {}, size: {} }}", io.flags, io.offset, io.size),
        }
    }
}

impl PendingIo {
    pub(crate) fn from_iodesc(iod: &ublksrv_io_desc) -> Self {
        let size = iod.nr_sectors << 9;
        let offset = iod.start_sector << 9;
        match iod.op_flags & 0xff {
            libublk::sys::UBLK_IO_OP_WRITE => {},
            libublk::sys::UBLK_IO_OP_FLUSH => {
                return Self::Flush(FlushIo { flags: iod.op_flags, size: size, offset: offset, });
            },
            _ => { panic!("op {:#02x} not implement", iod.op_flags & 0xff); },
        }

        // op WRITE
        let data = IoBuf::new(size as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(iod.addr as *const u8, data.as_mut_ptr(), size as usize);
        }
        Self::Write(WriteIo {
            flags: iod.op_flags,
            size: size,
            offset: offset,
            data: data,
        })
    }

    #[inline]
    pub(crate) fn offset(&self) -> u64 {
        match self {
            Self::Write(io) => io.offset,
            Self::Flush(io) => io.offset,
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Write(io) => io.size as usize,
            Self::Flush(io) => io.size as usize,
        }
    }

    #[inline]
    pub(crate) fn data_size(&self) -> usize {
        match self {
            Self::Write(io) => io.data.len(),
            Self::Flush(_) => 0,
        }
    }

    #[inline]
    pub(crate) fn as_ref(&self) -> &[u8] {
        match self {
            Self::Write(io) => &(io.data),
            Self::Flush(_) => panic!("can not deref FlushIo"),
        }
    }
}

pub(crate) struct LocalPendingBlocksPool {
    blocks: Vec<PendingIo>,
    // size in bytes
    max_capacity: usize,
    current_capacity: usize,
    tx: channel::Sender<Vec<PendingIo>>,
}

impl fmt::Debug for LocalPendingBlocksPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalPendingBlocksPool {{ blocks: {}, available capacity: {} }}", self.blocks.len(), self.avail_capacity())
    }
}

impl LocalPendingBlocksPool {
    pub(crate) fn new(max_capacity: usize, tx: channel::Sender<Vec<PendingIo>>) -> Self {
        Self {
            blocks: Vec::new(),
            max_capacity: max_capacity,
            current_capacity: 0,
            tx: tx,
        }
    }

    pub(crate) fn get_count(&self) -> usize {
        self.blocks.len()
    }

    pub(crate) fn avail_capacity(&self) -> usize {
        if self.max_capacity >= self.current_capacity {
            return self.max_capacity - self.current_capacity;
        }
        0
    }

    pub(crate) fn append(&mut self, pio: PendingIo) {
        self.blocks.push(pio);
    }

    // push pending io from local to central
    //
    // in 3 cases:
    // 1. no enough avail mem in local pool
    // 2. 1s timeout
    // 3. a flush op completed
    pub(crate) fn propagate(&mut self) {
        if self.blocks.len() == 0 {
            return;
        }
        let mut v = Vec::new();
        std::mem::swap(&mut v, &mut self.blocks);
        let _ = self.tx.send_blocking(v);
        self.current_capacity = 0;
    }
}

pub(crate) struct TgtPendingBlocksPool<T> {
    replica_path: String,
    replica_device: T,
    rx: channel::Receiver<Vec<PendingIo>>,
    tx: channel::Sender<Vec<PendingIo>>,
    pending_queue: Vec<PendingIo>,
    pending_bytes: usize,
    max_capacity: usize,
    meta_dev_desc: MetaDeviceDesc,
    dev_id: u32,
}

impl<T> TgtPendingBlocksPool<T> {
    pub(crate) fn new(max_capacity: usize, replica_path: &str, replica_device: T, meta_dev_desc: MetaDeviceDesc, dev_id: u32) -> Self
        where T: Replica + 'static
    {
        let (tx, rx) = channel::unbounded();
        Self {
            rx: rx,
            tx: tx,
            pending_queue: Vec::new(),
            pending_bytes: 0,
            max_capacity: max_capacity,
            replica_path: replica_path.to_string(),
            replica_device: replica_device,
            meta_dev_desc,
            dev_id,
        }
    }

    pub(crate) fn get_stats(&self) -> (usize, usize) {
        (self.pending_bytes, self.max_capacity)
    }

    pub(crate) fn get_tx_chan(&self) -> channel::Sender<Vec<PendingIo>> {
        self.tx.clone()
    }

    pub(crate) async fn main_loop(pool: Rc<RefCell<Self>>, state: Rc<GlobalTgtState>,
            region: Rc<Region>, _recover: Rc<RecoverCtrl>)
        where T: Replica + 'static
    {
        info!("TgtPendingBlocksPool started with:");
        info!("  - state {:?}", state);
        info!("  - region {:?}", region);
        let rx = pool.borrow().rx.clone();
        let replica_device = pool.borrow().replica_device.dup().await;
        while let Ok(mut v) = rx.recv().await {
            let total_bytes: usize = v.iter().map(|pio| pio.size()).sum();
            pool.borrow_mut().pending_bytes += total_bytes;
            pool.borrow_mut().pending_queue.append(&mut v);
            if pool.borrow().pending_bytes >= pool.borrow().max_capacity {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes exceed max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                // TODO: change process to
                // 1. disable logging
                // 2. wait incoming queue empty?
                // 3. write_to_replica
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();
                pool.borrow_mut().pending_bytes = 0;

                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);

                state.set_logging_disable();

            } else if pool.borrow().pending_bytes >= pool.borrow().max_capacity / 2 {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes exceed 1/2 max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                // TODO: change process to
                // 1. add periodic flusher
                // 2. find last FLUSH in the queue and take out, leave remains in the queue
                // 3. write_to_replica
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();
                pool.borrow_mut().pending_bytes = 0;

                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);
            }
            // yield to prevent long term occupation of this task
            smol::future::yield_now().await;
        }
        info!("TgtPendingBlocksPool quit");
    }

    pub(crate) async fn periodic(pool: Rc<RefCell<Self>>)
        where T: Replica + 'static
    {
        // create a dedicate intance of replica deivce instance
        let replica_device = pool.borrow().replica_device.dup().await;
        loop {
            smol::Timer::after(std::time::Duration::from_secs(1)).await;
            let pending_bytes = pool.borrow().pending_bytes;
            let pending_queue_len = pool.borrow().pending_queue.len();
            if pending_bytes > 0 {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes, periodic log pending io",
                    pending_queue_len, pending_bytes);
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();
                pool.borrow_mut().pending_bytes = 0;

                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);
            }
        }
    }

    pub(crate) async fn periodic_replica_flush(pool: Rc<RefCell<Self>>)
        where T: Replica + 'static
    {
        // create a dedicate intance of replica deivce instance
        let replica_device = pool.borrow().replica_device.dup().await;
        let meta_dev_desc = pool.borrow().meta_dev_desc.clone();
        let mut meta_dev = MetaDevice::open(&meta_dev_desc).await;
        let entry = meta_dev.flush_log.last_entry();
        let mut last_replica_ondisk_cno = replica_device.last_cno().await;
        let mut last_primary_metadata_cno = entry.cno;
        loop {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
            let now = SystemTime::now();
            let cno = replica_device.flush().await.expect("replica deivce flush failed");
            debug!("TgtPendingBlocksPool - periodic replica flush done - segid: {}, cost: {:?}", cno, now.elapsed().unwrap());
            if last_replica_ondisk_cno < cno && last_primary_metadata_cno < cno {
                // only sync log entry cno for an effective replica flush
                let _ = meta_dev.flush_log_sync(cno).await;
                last_primary_metadata_cno = cno;
                last_replica_ondisk_cno = cno;
            }
        }
    }

    pub(crate) fn start(self, unix_sock: PathBuf, state: GlobalTgtState, region: Region, recover: RecoverCtrl) -> std::thread::JoinHandle<()>
        where T: Replica + 'static
    {
        std::thread::spawn(move || {
            let mut f_vec = Vec::new();
            let exec = LocalExecutor::new();

            let rc_pool = Rc::new(RefCell::new(self));
            let rc_state = Rc::new(state);
            let rc_region = Rc::new(region);
            let rc_recover = Rc::new(recover);

            let c_pool = rc_pool.clone();
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            f_vec.push(exec.spawn(async move {
                Self::main_loop(c_pool, c_state, c_region, c_recover).await;
            }));

            let c_pool = rc_pool.clone();
            f_vec.push(exec.spawn(async move {
                Self::periodic(c_pool).await;
            }));

            let c_pool = rc_pool.clone();
            f_vec.push(exec.spawn(async move {
                Self::periodic_replica_flush(c_pool).await;
            }));

            let rc_exec = Rc::new(exec);

            let c_recover = rc_recover.clone();
            let c_exec = rc_exec.clone();
            let c_pool = rc_pool.clone();
            f_vec.push(rc_exec.spawn(async move {
                let replica = c_pool.borrow().replica_device.dup().await;
                c_recover.main_loop(replica, c_exec).await;
            }));

            let cmd_chan = CommandChannel::new(unix_sock.as_path());
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            let c_pool = rc_pool.clone();
            let c_exec = rc_exec.clone();
            f_vec.push(rc_exec.spawn(async move {
                cmd_chan.main_handler(c_state, c_region, c_recover, c_pool, c_exec).await;
            }));

            smol::block_on(async { loop { rc_exec.tick().await }});
        })
    }

    fn stop_ublk_dev(dev_id: u32) -> Result<(), UblkError> {
        let ctrl = UblkCtrl::new_simple(dev_id as i32)?;
        ctrl.stop_dev()?;
        Ok(())
    }

    async fn inner_stop(pool: Rc<RefCell<Self>>, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>) -> Result<(), UblkError> {
        let dev_id = pool.borrow().dev_id;

        // stop ublk dev in a blocking thread
        smol::unblock(move || {
            Self::stop_ublk_dev(dev_id)
        }).await?;

        // wait any recover complete
        while state.is_recovery() {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        // wait pending io channel empty
        let rx = pool.borrow().rx.clone();
        while !rx.is_empty() {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        // wait pending queue empty
        while pool.borrow().pending_queue.len() > 0 {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        // TODO: check no activity on replica device

        // finally close superblock
        let meta_dev_desc = pool.borrow().meta_dev_desc.clone();
        let mut meta_dev = MetaDevice::open(&meta_dev_desc).await;
        let _ = meta_dev.sb_close_sync().await;

        std::process::exit(0);
        Ok(())
    }

    pub(crate) async fn stop(pool: Rc<RefCell<Self>>, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>) {
        loop {
            match Self::inner_stop(pool.clone(), state.clone(), region.clone(), recover.clone()).await {
                Ok(_) => { break; },
                Err(UblkError::UringIoQueued) => {
                    smol::Timer::after(std::time::Duration::from_secs(5)).await;
                    continue;
                },
                Err(e) => {
                    warn!("stop ublk device error {}", e);
                    break;
                }
            }
        }
    }
}
