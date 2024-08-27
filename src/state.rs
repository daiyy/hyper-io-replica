use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Barrier;
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
}

impl LocalTgtState {
    pub(crate) fn new(global: Arc<AtomicU64>, barrier: Arc<Barrier>) -> Self {
        Self {
            inner: global.load(Ordering::SeqCst),
            changed: false,
            global: global,
            barrier: barrier,
        }
    }

    // download global state to local
    // return if or not we need to wait on barrier
    #[inline]
    fn download(&mut self) -> bool {
        let stat = self.global.load(Ordering::SeqCst);
        if self.inner != stat {
            self.inner = stat;
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
    pub(crate) fn sync(&mut self) {
        if self.changed {
            self.upload();
            self.changed = false;
            // FIXME: can not wait barrier, main loop would be block for 20s
            //self.barrier.wait();
            return;
        }
        if self.download() {
            // FIXME: can not wait barrier, main loop would be block for 20s
            //self.barrier.wait();
        }
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
        write!(f, "GlobalTgtState {{ logging_enabled: {} }}", logging_enabled)
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
}

#[inline]
pub(crate) fn local_state_sync() {
    LOCAL_STATE.with(|state| {
        state.borrow_mut().sync()
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
