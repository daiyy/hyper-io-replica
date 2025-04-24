use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use log::info;
use crate::io_replica::LOCAL_STATE;

pub(crate) const TGT_STATE_LOGGING_ENABLED: u64 = 0b0001;
pub(crate) const TGT_STATE_RECOVERY_REVERSE_FULL: u64 = 0b0010;
pub(crate) const TGT_STATE_RECOVERY_FORWARD_FULL: u64 = 0b0001_0000;
pub(crate) const TGT_STATE_RECOVERY_FORWARD_PART: u64 = 0b0010_0000;
pub(crate) const TGT_STATE_RECOVERY_FORWARD_FINAL: u64 = 0b1000_0000; // final stage: wait on region clean
pub(crate) const TGT_STATE_RECOVERY_MASK: u64 = 0b1111_1110;

pub(crate) fn tgt_state_to_string(state: u64) -> String {
	let logging = state & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED;
	let reverse_full = state & TGT_STATE_RECOVERY_REVERSE_FULL == TGT_STATE_RECOVERY_REVERSE_FULL;
	let forward_full = state & TGT_STATE_RECOVERY_FORWARD_FULL == TGT_STATE_RECOVERY_FORWARD_FULL;
	let forward_part = state & TGT_STATE_RECOVERY_FORWARD_PART == TGT_STATE_RECOVERY_FORWARD_PART;
	let forward_final = state & TGT_STATE_RECOVERY_FORWARD_FINAL == TGT_STATE_RECOVERY_FORWARD_FINAL;

	let mut v = Vec::new();
	if logging { v.push("LoggingEnabled".to_string()); } else { v.push("LoggingDisabled".to_string()); }
	if reverse_full { v.push("RecoverReverseFull".to_string()); }
	if forward_full { v.push("RecoverForwardFull".to_string()); }
	if forward_part { v.push("RecoverForwardPart".to_string()); }
	if forward_final { v.push("RecoverForwardFinal".to_string()); }

	v.join(" | ")
}

pub(crate) struct LocalTgtState {
    inner: u64,     // local copy of tgt state
    changed: bool,  // if local state changed
    global: Arc<AtomicU64>,
    qid: u16,
}

impl LocalTgtState {
    pub(crate) fn new(qid: u16, global: Arc<AtomicU64>) -> Self {
        Self {
            inner: global.load(Ordering::SeqCst),
            changed: false,
            global: global,
            qid: qid,
        }
    }

    // download global state to local
    // return if or not we need to wait on barrier
    #[inline]
    fn download(&mut self, wait_time: &Duration) -> bool {
        let state = self.global.load(Ordering::SeqCst);
        if self.inner != state {
            info!("qid({}) local state wait {:?} changed from {} => {}", self.qid, wait_time, self.inner, state);
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
            return true;
        }
        if self.download(wait_time) {
            return true;
        }
        false
    }

    pub(crate) fn is_logging_enabled(&self) -> bool {
        self.inner & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED
    }

    pub(crate) fn is_recovery_reverse_full(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_REVERSE_FULL == TGT_STATE_RECOVERY_REVERSE_FULL
    }

    pub(crate) fn is_recovery_forward_full(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_FORWARD_FULL == TGT_STATE_RECOVERY_FORWARD_FULL
    }

    pub(crate) fn is_recovery_forward_part(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_FORWARD_PART == TGT_STATE_RECOVERY_FORWARD_PART
    }

    #[allow(dead_code)]
    pub(crate) fn is_recovery_forward_final(&self) -> bool {
        self.inner & TGT_STATE_RECOVERY_FORWARD_FINAL == TGT_STATE_RECOVERY_FORWARD_FINAL
    }

    pub(crate) fn is_recovery(&self) -> bool {
        (self.inner & TGT_STATE_RECOVERY_MASK) > 0
    }
}

#[derive(Clone)]
pub(crate) struct GlobalTgtState {
    inner: Arc<AtomicU64>,
}

impl fmt::Debug for GlobalTgtState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.inner.load(Ordering::SeqCst);
        let logging_enabled = state & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED;
        let recovery_reverse_full = state & TGT_STATE_RECOVERY_REVERSE_FULL == TGT_STATE_RECOVERY_REVERSE_FULL;
        let recovery_forward_full = state & TGT_STATE_RECOVERY_FORWARD_FULL == TGT_STATE_RECOVERY_FORWARD_FULL;
        let recovery_forward_part = state & TGT_STATE_RECOVERY_FORWARD_PART == TGT_STATE_RECOVERY_FORWARD_PART;
        let recovery_forward_final = state & TGT_STATE_RECOVERY_FORWARD_FINAL == TGT_STATE_RECOVERY_FORWARD_FINAL;
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
        } else if recovery_forward_final {
            msg.push("recovery_forward_final: true");
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
        let _ = self.inner.fetch_and(state, Ordering::SeqCst);
    }

    pub(crate) fn set_logging_enable(&self) {
        let state = TGT_STATE_LOGGING_ENABLED;
        let _ = self.inner.fetch_or(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_reverse_full(&self) {
        let state = TGT_STATE_RECOVERY_REVERSE_FULL;
        let _ = self.inner.fetch_or(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_forward_full(&self) {
        let state = TGT_STATE_RECOVERY_FORWARD_FULL;
        let _ = self.inner.fetch_or(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_forward_part(&self) {
        let state = TGT_STATE_RECOVERY_FORWARD_PART;
        let _ = self.inner.fetch_or(state, Ordering::SeqCst);
    }

    pub(crate) fn set_recovery_forward_final(&self) {
        let state = TGT_STATE_RECOVERY_FORWARD_FINAL;
        let _ = self.inner.fetch_or(state, Ordering::SeqCst);
    }

    pub(crate) fn clear_all_recover_bits(&self) -> u64 {
        let state = !(TGT_STATE_RECOVERY_FORWARD_FULL | TGT_STATE_RECOVERY_FORWARD_PART |
            TGT_STATE_RECOVERY_FORWARD_FINAL | TGT_STATE_RECOVERY_REVERSE_FULL);
        let _ = self.inner.fetch_and(state, Ordering::SeqCst);
        self.inner.load(Ordering::SeqCst)
    }

    pub(crate) fn is_logging_enabled(&self) -> bool {
        self.inner.load(Ordering::SeqCst) & TGT_STATE_LOGGING_ENABLED == TGT_STATE_LOGGING_ENABLED
    }

    pub(crate) fn is_recovery(&self) -> bool {
        let recovery_state = TGT_STATE_RECOVERY_FORWARD_FULL | TGT_STATE_RECOVERY_FORWARD_PART |
            TGT_STATE_RECOVERY_FORWARD_FINAL | TGT_STATE_RECOVERY_REVERSE_FULL;
        self.inner.load(Ordering::SeqCst) & recovery_state > 0
    }

    pub(crate) fn is_recovery_forward_final(&self) -> bool {
        self.inner.load(Ordering::SeqCst) & TGT_STATE_RECOVERY_FORWARD_FINAL == TGT_STATE_RECOVERY_FORWARD_FINAL
    }

    pub(crate) fn is_not_recovery_and_logging_enabled(&self) -> bool {
        self.inner.load(Ordering::SeqCst) == TGT_STATE_LOGGING_ENABLED
    }

    pub(crate) fn get_raw(&self) -> u64 {
        self.inner.load(Ordering::SeqCst)
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

#[allow(dead_code)]
#[inline]
pub(crate) fn local_state_recovery_forward_full() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_forward_full()
    })
}

#[allow(dead_code)]
#[inline]
pub(crate) fn local_state_recovery_forward_part() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_forward_part()
    })
}

#[allow(dead_code)]
#[inline]
pub(crate) fn local_state_recovery_reverse_full() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery_reverse_full()
    })
}

#[inline]
pub(crate) fn local_state_recovery() -> bool {
    LOCAL_STATE.with(|state| {
        state.borrow().is_recovery()
    })
}
