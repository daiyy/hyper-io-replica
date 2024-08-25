use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use crate::io_replica::LOCAL_STATE;

const TGT_STATE_LOGGING_ENABLED: u64 = 0x1;

pub(crate) struct LocalTgtState {
    inner: u64,     // local copy of tgt state
    changed: bool,  // if local state changed
    global: Arc<AtomicU64>,
}

impl LocalTgtState {
    pub(crate) fn new(global: Arc<AtomicU64>) -> Self {
        Self {
            inner: global.load(Ordering::SeqCst),
            changed: false,
            global: global,
        }
    }

    // download global state to local
    #[inline]
    fn download(&mut self) {
        self.inner = self.global.load(Ordering::SeqCst);
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
            return;
        }
        self.download();
    }

    pub(crate) fn is_logging_enabled(&self) -> bool {
        self.inner & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED
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
