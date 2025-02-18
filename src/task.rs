/// Define all tasks
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
