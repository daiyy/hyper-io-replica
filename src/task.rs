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

// high 32 bits for busy | low 32 bits for start
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

    pub(crate) fn set_stop(&self, id: TaskId) {
        let _ = self.inner.fetch_and(!(id as u64), Ordering::SeqCst);
    }

    pub(crate) fn is_open(&self, id: TaskId) -> bool {
        let state = self.inner.load(Ordering::SeqCst);
        let task_id = id as u64;
        state & task_id == task_id
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

    pub(crate) fn set_busy(&self, id: TaskId) {
        let _ = self.inner.fetch_or((id as u64) << 32, Ordering::SeqCst);
    }

    pub(crate) fn clear_busy(&self, id: TaskId) {
        let _ = self.inner.fetch_and(!((id as u64) << 32), Ordering::SeqCst);
    }

    pub(crate) async fn wait_on_all_tasks_idle(&self) {
        loop {
            let state = self.inner.load(Ordering::SeqCst);
            if (state >> 32) == 0 {
                // if all tasks are not in busy
                break;
            } else {
                smol::Timer::after(Duration::from_secs(5)).await;
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
        let _ = replica_device;
        loop {
            smol::Timer::after(std::time::Duration::from_secs(1)).await;
            let stats = pool.borrow().get_stats();
            if stats.is_active() {
                debug!("TgtPendingBlocksPool - {}", stats);
            }
        }
    }

    pub(crate) async fn periodic_replica_flush(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, task_state: TaskState) {
        // create a dedicate intance of replica deivce instance
        let replica_device = pool.borrow().replica_device.dup().await;
        let entry_cno = pool.borrow().meta_dev.borrow().flush_log.last_entry().cno;
        let mut last_replica_ondisk_cno = replica_device.last_cno().await;
        let mut last_primary_metadata_cno = entry_cno;
        task_state.wait_on_tgt_pool_start().await;
        task_state.set_start(TaskId::PeriodicReplicaFlush);
        loop {
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
            if pool.borrow().is_replica_busy() { continue; }
            if !task_state.is_open(TaskId::PeriodicReplicaFlush) { continue; }
            task_state.set_busy(TaskId::PeriodicReplicaFlush);
            let now = SystemTime::now();
            let cno = replica_device.flush().await.expect("replica deivce flush failed");
            if last_replica_ondisk_cno < cno && last_primary_metadata_cno < cno {
                debug!("TgtPendingBlocksPool periodic task replica flush - done with segid: {}, cost: {:?}", cno, now.elapsed().unwrap());
                // only sync log entry cno for an effective replica flush
                let _ = pool.borrow().meta_dev.borrow_mut().flush_log_sync(cno).await;
                last_primary_metadata_cno = cno;
                last_replica_ondisk_cno = cno;
                #[cfg(feature="piopr")] {
                    let mut v_iometa = Vec::new();
                    v_iometa.append(pool.borrow_mut().flush_queue.as_mut());
                    pool.borrow().piopr.dec_pending(v_iometa).expect("failed to dec pending for piopr");
                }
            }
            task_state.clear_busy(TaskId::PeriodicReplicaFlush);
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
            let c_pool = rc_pool.clone();
            let c_region = rc_region.clone();
            let c_task_state = task_state.clone();
            f_vec.push(rc_exec.spawn(async move {
                c_recover.main_loop(c_pool, c_region, c_task_state).await;
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

    async fn inner_stop(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, state: Rc<GlobalTgtState>, region: Rc<Region>, _recover: Rc<RecoverCtrl>, task_state: TaskState) -> Result<(), UblkError> {
        let dev_id = pool.borrow().dev_id;

        // stop ublk dev in a blocking thread
        smol::unblock(move || {
            Self::stop_ublk_dev(dev_id)
        }).await?;
        debug!("TaskManager - stop wait - ublk dev stopped");
        smol::future::yield_now().await;

        // wait any recover complete
        while state.is_recovery() {
            debug!("TaskManager - stop wait - is recovering");
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }

        // wait 2s for local pool send data to channel
        smol::Timer::after(std::time::Duration::from_secs(2)).await;
        smol::future::yield_now().await;

        // wait pending io channel empty
        let rx = pool.borrow().rx.clone();
        while !rx.is_empty() || pool.borrow().get_stats().is_active() {
            debug!("TaskManager - stop wait - queue is empty {}, pool is active {}", rx.is_empty(), pool.borrow().get_stats().is_active());
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }
        smol::future::yield_now().await;

        // stop flusher
        task_state.set_stop(TaskId::PeriodicReplicaFlush);
        smol::future::yield_now().await;

        task_state.wait_on_all_tasks_idle().await;
        smol::future::yield_now().await;

        let replica = pool.borrow().replica_device.dup().await;
        while replica.is_active() {
            debug!("TaskManager - stop wait - replica is active");
            smol::Timer::after(std::time::Duration::from_secs(5)).await;
        }
        smol::future::yield_now().await;

        // open meta dev for last flush
        let entry_cno = pool.borrow().meta_dev.borrow().flush_log.last_entry().cno;
        let last_primary_metadata_cno = entry_cno;
        let cno = replica.close().await?;
        if  last_primary_metadata_cno < cno {
            let _ = pool.borrow().meta_dev.borrow_mut().flush_log_sync(cno).await;
        }

        let dirty_region = region.collect();
        if dirty_region.len() > 0 {
            warn!("TaskManager - before exit - dirty region {:?}", dirty_region);
        }

        #[cfg(feature="piopr")]
        debug!("TaskManager - before exit - piopr");
        #[cfg(feature="piopr")]
        debug!("{}", pool.borrow().piopr);

        // close preg
        let _ = pool.borrow().meta_dev.borrow_mut().preg.close().await;
        #[cfg(feature="piopr")]
        let _ = pool.borrow().meta_dev.borrow_mut().preg2.close().await;
        // finally close superblock
        let _ = pool.borrow().meta_dev.borrow_mut().sb_close_sync().await;

        std::process::exit(0);
    }

    pub(crate) async fn stop(pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, state: Rc<GlobalTgtState>, region: Rc<Region>, recover: Rc<RecoverCtrl>, task_state: TaskState) {
        loop {
            match Self::inner_stop(pool.clone(), state.clone(), region.clone(), recover.clone(), task_state.clone()).await {
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
