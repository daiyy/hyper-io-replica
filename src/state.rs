use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Barrier;
use std::time::Duration;
use log::info;
use crate::io_replica::LOCAL_STATE;

pub(crate) const TGT_STATE_LOGGING_ENABLED: u64 = 0b0001;
pub(crate) const TGT_STATE_RECOVERY_FORWARD_FULL: u64 = 0b0010;
pub(crate) const TGT_STATE_RECOVERY_FORWARD_PART: u64 = 0b0100;
pub(crate) const TGT_STATE_RECOVERY_REVERSE_FULL: u64 = 0b1000;

pub(crate) struct LocalTgtState {
    inner: u64,     // local copy of tgt state
    changed: bool,  // if local state changed
    global: Arc<AtomicU64>,
    barrier: Arc<Barrier>,
    qid: u16,
}

impl LocalTgtState {
    pub(crate) fn new(qid: u16, global: Arc<AtomicU64>, barrier: Arc<Barrier>) -> Self {
        Self {
            inner: global.load(Ordering::SeqCst),
            changed: false,
            global: global,
            barrier: barrier,
            qid: qid,
        }
    }

    // download global state to local
    // return if or not we need to wait on barrier
    #[inline]
    fn download(&mut self, wait_time: &Duration) -> bool {
        let state = self.global.load(Ordering::SeqCst);
        if self.inner != state {
            info!("qid({}) local state wait {:?} changed from {} => {}", self.qid, wait_time, state, self.inner);
            self.inner = state;
            return true;
        }
        false
    }

    // upload local state to global
    #[inline]
    fn upload(&mut self) {
        self.global.store(self.inner, Ordering::SeqCst);
    }

    // sync local state with global
    // return:
    //   - if state changed
    pub(crate) fn sync(&mut self, wait_time: &Duration) -> bool {
        if self.changed {
            self.upload();
            self.changed = false;
            // FIXME: can not wait barrier, main loop would be block for 20s
            //self.barrier.wait();
            return true;
        }
        if self.download(wait_time) {
            // FIXME: can not wait barrier, main loop would be block for 20s
            //self.barrier.wait();
            return true;
        }
        false
    }

    pub(crate) fn is_logging_enabled(&self) -> bool {
        self.inner & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED
    }

    pub(crate) fn is_recovery_forward_full(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_FORWARD_FULL == TGT_STATE_RECOVERY_FORWARD_FULL
    }

    pub(crate) fn is_recovery_forward_part(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_FORWARD_PART == TGT_STATE_RECOVERY_FORWARD_PART
    }

    pub(crate) fn is_recovery_reverse_full(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_REVERSE_FULL == TGT_STATE_RECOVERY_REVERSE_FULL
    }
}

pub(crate) struct GlobalTgtState {
    inner: Arc<AtomicU64>,
}

impl fmt::Debug for GlobalTgtState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.load(Ordering::SeqCst);
        let logging_enabled = state & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED;
        let recovery_forward_full = state & TGT_STATE_RECOVERY_FORWARD_FULL == TGT_STATE_RECOVERY_FORWARD_FULL;
        let recovery_forward_part = state & TGT_STATE_RECOVERY_FORWARD_PART == TGT_STATE_RECOVERY_FORWARD_PART;
        let recovery_reverse_full = state & TGT_STATE_RECOVERY_REVERSE_FULL == TGT_STATE_RECOVERY_REVERSE_FULL;
        let mut msg = Vec::new();
        if logging_enabled {
            msg.push("logging_enabled: true");
        } else {
            msg.push("logging_enabled: false");
        }
        if recovery_forward_full {
            msg.push("recovery_forward_full: true");
        } else if recovery_forward_part {
            msg.push("recovery_forward_part: true");
        } else if recovery_reverse_full {
            msg.push("recovery_reverse_full: true");
        }
        write!(f, "GlobalTgtState {{ {} }}", msg.join(", "))
    }
}

impl GlobalTgtState {
    pub(crate) fn new() -> Self {
        let state = TGT_STATE_LOGGING_ENABLED;
        Self {
            inner: Arc::new(AtomicU64::new(state)),
        }
    }

    pub(crate) fn state_clone(&self) -> Arc<AtomicU64> {
        self.inner.clone()
    }

    pub(crate) fn set_logging_disable(&self) {
        let state = !TGT_STATE_LOGGING_ENABLED;
        self.inner.fetch_and(state, Ordering::SeqCst);
    }

    pub(crate) fn set_logging_enable(&self) {
        let state = TGT_STATE_LOGGING_ENABLED;
        self.inner.store(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_forward_full(&self) {
        let state = TGT_STATE_RECOVERY_FORWARD_FULL;
        self.inner.store(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_forward_part(&self) {
        let state = TGT_STATE_RECOVERY_FORWARD_PART;
        self.inner.store(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_reverse_full(&self) {
        let state = TGT_STATE_RECOVERY_REVERSE_FULL;
        self.inner.store(state, Ordering::SeqCst);
    }
}

#[inline]
pub(crate) fn local_state_sync(wait_time: &Duration) -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow_mut().sync(wait_time)
    })
}

#[inline]
pub(crate) fn local_state_logging_enabled() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_logging_enabled()
    })
}

#[inline]
pub(crate) fn local_state_recovery_forward_full() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_forward_full()
    })
}

#[inline]
pub(crate) fn local_state_recovery_forward_part() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_forward_part()
    })
}

#[inline]
pub(crate) fn local_state_recovery_reverse_full() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_reverse_full()
    })
}
