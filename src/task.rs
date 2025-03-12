/// Define all tasks
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::marker::PhantomData;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::{SystemTime, Duration};
use std::path::PathBuf;
use log::{debug, warn};
use libublk::{ctrl::UblkCtrl, UblkError};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::mgmt::CommandChannel;
use crate::replica::Replica;
use crate::pool::TgtPendingBlocksPool;
use crate::device::MetaDevice;

#[repr(u64)]
pub(crate) enum TaskId {
    Pool = 0b0000_0001,
    Periodic = 0b0000_0010,
    PeriodicReplicaFlush = 0b0000_0100,
    Recover = 0b0000_1000,
    MgmtChannel = 0b0001_0000,
}

#[derive(Clone)]
pub(crate) struct TaskState {
    inner: Arc<AtomicU64>,
}

impl TaskState {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(AtomicU64::new(0)),
        }
    }

    fn all_tasks() -> u64 {
        TaskId::Pool as u64
            | TaskId::Periodic as u64
            | TaskId::PeriodicReplicaFlush as u64
            | TaskId::Recover as u64
            | TaskId::MgmtChannel as u64
    }

    pub(crate) fn set_start(&self, id: TaskId) {
        let _ = self.inner.fetch_or(id as u64, Ordering::SeqCst);
    }

    pub(crate) fn is_all_started(&self) -> bool {
       if self.inner.load(Ordering::SeqCst) == Self::all_tasks() {
           return true;
       }
       false
    }

    pub(crate) async fn wait_on_tgt_pool_start(&self) {
        loop {
            let state = self.inner.load(Ordering::SeqCst);
            if state & TaskId::Pool as u64 > 0 {
                // if pool task is started
                break;
            } else if state == 0 {
                smol::Timer::after(Duration::from_millis(10)).await;
            } else {
                panic!("invalid TaskState {state:b} while wait on tgt pool start");
            }
        }
    }
}

pub struct TaskManager<T> {
    phantom: PhantomData<T>,
}

impl<T: Replica + 'static> TaskManager<T> {
    pub(crate) async fn periodic(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, task_state: TaskState) {
        // create a dedicate intance of replica deivce instance
        let replica_device = pool.borrow().replica_device.dup().await;
        task_state.wait_on_tgt_pool_start().await;
        task_state.set_start(TaskId::Periodic);
        loop {
            smol::Timer::after(std::time::Duration::from_secs(1)).await;
            if replica_device.is_active() { continue; }
            let pending_bytes = pool.borrow().pending_bytes;
            let pending_queue_len = pool.borrow().pending_queue.len();
            let staging_data_queue_len = pool.borrow().staging_data_queue.len();
            if pending_bytes > 0 {
                debug!("TgtPendingBlocksPool - {} of pending IO, {} of staging data pending IO, total {} bytes, periodic log pending io",
                    pending_queue_len, staging_data_queue_len, pending_bytes);
                // take all in staging data queue
                let mut v = pool.borrow_mut().staging_data_queue.drain(..).collect();
                pool.borrow_mut().staging_data_queue_bytes = 0;
                pool.borrow_mut().pending_queue.append(&mut v);
                // tak all from pending queue
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();

                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);
                pool.borrow_mut().pending_bytes = 0;
                debug!("TgtPendingBlocksPool - {} bytes, periodic log pending io Done", pending_bytes);
            }
        }
    }

    pub(crate) async fn periodic_replica_flush(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, task_state: TaskState) {
        // create a dedicate intance of replica deivce instance
        let replica_device = pool.borrow().replica_device.dup().await;
        let meta_dev_desc = pool.borrow().meta_dev_desc.clone();
        let mut meta_dev = MetaDevice::open(&meta_dev_desc).await;
        let entry = meta_dev.flush_log.last_entry();
        let mut last_replica_ondisk_cno = replica_device.last_cno().await;
        let mut last_primary_metadata_cno = entry.cno;
        task_state.wait_on_tgt_pool_start().await;
        task_state.set_start(TaskId::PeriodicReplicaFlush);
        loop {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
            if replica_device.is_active() { continue; }
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

    pub(crate) fn start(pool: TgtPendingBlocksPool<T>, unix_sock: PathBuf, state: GlobalTgtState, region: Region,
            recover: RecoverCtrl, task_state: TaskState) -> std::thread::JoinHandle<()>
    {
        std::thread::spawn(move || {
            let mut f_vec = Vec::new();
            let exec = smol::LocalExecutor::new();

            let rc_pool = Rc::new(RefCell::new(pool));
            let rc_state = Rc::new(state);
            let rc_region = Rc::new(region);
            let rc_recover = Rc::new(recover);

            let c_pool = rc_pool.clone();
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            let c_task_state = task_state.clone();
            f_vec.push(exec.spawn(async move {
                TgtPendingBlocksPool::main_loop(c_pool, c_state, c_region, c_recover, c_task_state).await;
            }));

            let c_pool = rc_pool.clone();
            let c_task_state = task_state.clone();
            f_vec.push(exec.spawn(async move {
                Self::periodic(c_pool, c_task_state).await;
            }));

            let c_pool = rc_pool.clone();
            let c_task_state = task_state.clone();
            f_vec.push(exec.spawn(async move {
                Self::periodic_replica_flush(c_pool, c_task_state).await;
            }));

            let rc_exec = Rc::new(exec);

            let c_recover = rc_recover.clone();
            let c_exec = rc_exec.clone();
            let c_pool = rc_pool.clone();
            let c_task_state = task_state.clone();
            f_vec.push(rc_exec.spawn(async move {
                let replica = c_pool.borrow().replica_device.dup().await;
                c_recover.main_loop(replica, c_exec, c_task_state).await;
            }));

            let cmd_chan = CommandChannel::new(unix_sock.as_path());
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            let c_pool = rc_pool.clone();
            let c_exec = rc_exec.clone();
            let c_task_state = task_state.clone();
            f_vec.push(rc_exec.spawn(async move {
                cmd_chan.main_handler(c_state, c_region, c_recover, c_pool, c_exec, c_task_state).await;
            }));

            smol::block_on(async { loop { rc_exec.tick().await }});
        })
    }

    fn stop_ublk_dev(dev_id: u32) -> Result<(), UblkError> {
        let ctrl = UblkCtrl::new_simple(dev_id as i32)?;
        ctrl.stop_dev()?;
        Ok(())
    }

    async fn inner_stop(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, state: Rc<GlobalTgtState>, _region: Rc<Region>, _recover: Rc<RecoverCtrl>) -> Result<(), UblkError> {
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

        // wait pending bytes to 0
        while pool.borrow().pending_bytes > 0 {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        let replica = pool.borrow().replica_device.dup().await;
        while replica.is_active() {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        let _ = replica.close().await;

        // finally close superblock
        let meta_dev_desc = pool.borrow().meta_dev_desc.clone();
        let mut meta_dev = MetaDevice::open(&meta_dev_desc).await;
        let _ = meta_dev.sb_close_sync().await;

        std::process::exit(0);
    }

    pub(crate) async fn stop(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>) {
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
