use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub(crate) struct IncrSeq {
    inner: Arc<AtomicU64>,
}

impl IncrSeq {
    pub(crate) fn new() -> Self {
        // reserve 0, start from 1
        Self { inner: Arc::new(AtomicU64::new(1)) }
    }

    pub(crate) fn next(&self, op_flags: u32) -> u64 {
        if op_flags & libublk::sys::UBLK_IO_F_META == libublk::sys::UBLK_IO_F_META ||
            op_flags & libublk::sys::UBLK_IO_OP_FLUSH == libublk::sys::UBLK_IO_OP_FLUSH
        {
            return self.inner.fetch_add(1, Ordering::SeqCst);
        }
        0
    }

    pub(crate) fn get(&self) -> u64 {
        self.inner.load(Ordering::SeqCst)
    }

    pub(crate) fn reset(&self) {
        self.inner.store(1, Ordering::SeqCst)
    }
}
