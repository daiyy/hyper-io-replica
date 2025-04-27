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

    #[inline]
    pub(crate) fn next(&self, op_flags: u32) -> u64 {
        let op = op_flags & 0xff;
        if op == libublk::sys::UBLK_IO_OP_WRITE {
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
