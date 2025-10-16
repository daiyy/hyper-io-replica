use std::io::Result;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use log::info;
use crate::pool::PendingIo;

pub(crate) mod blank;
pub(crate) mod file;
#[cfg(feature="blocking")]
pub(crate) mod s3;
#[cfg(feature="reactor")]
pub(crate) mod s3_reactor;

pub const REPLICA_STATE_OPENED: u64 = 0b0000_0001;
pub const REPLICA_STATE_READ: u64 = 0b0000_0010;
pub const REPLICA_STATE_WRITE: u64 = 0b0000_0100;
pub const REPLICA_STATE_FLUSH: u64 = 0b0000_1000;
pub const REPLICA_STATE_LOGGING: u64 = 0b0001_0000;

#[derive(Clone, Debug)]
pub struct ReplicaState {
    inner: Arc<AtomicU64>,
}

impl ReplicaState {
    pub fn new() -> Self {
        Self { inner: Arc::new(AtomicU64::new(0)) }
    }

    pub fn set_opened(&self) {
        if let Err(prev) = self.inner.compare_exchange(0, REPLICA_STATE_OPENED, Ordering::SeqCst, Ordering::SeqCst) {
            panic!("ReplicaState: {prev:b}, can not set to opened");
        }
    }

    pub fn set_read(&self) {
        let _ = self.inner.fetch_or(REPLICA_STATE_READ, Ordering::SeqCst);
    }

    pub fn clear_read(&self) {
        let _ = self.inner.fetch_and(!REPLICA_STATE_READ, Ordering::SeqCst);
    }

    pub fn set_write(&self) {
        let _ = self.inner.fetch_or(REPLICA_STATE_WRITE, Ordering::SeqCst);
    }

    pub fn clear_write(&self) {
        let _ = self.inner.fetch_and(!REPLICA_STATE_WRITE, Ordering::SeqCst);
    }

    pub fn set_flush(&self) {
        let _ = self.inner.fetch_or(REPLICA_STATE_FLUSH, Ordering::SeqCst);
    }

    pub fn clear_flush(&self) {
        let _ = self.inner.fetch_and(!REPLICA_STATE_FLUSH, Ordering::SeqCst);
    }

    pub fn set_logging(&self) {
        let _ = self.inner.fetch_or(REPLICA_STATE_LOGGING, Ordering::SeqCst);
    }

    pub fn clear_logging(&self) {
        let _ = self.inner.fetch_and(!REPLICA_STATE_LOGGING, Ordering::SeqCst);
    }

    // clear all state bits
    // return:
    //   true - success closed
    //   false - still have activities, user should wait and retry
    pub fn set_closed(&self) -> bool {
        match self.inner.compare_exchange(REPLICA_STATE_OPENED, 0, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => { return true; },
            Err(curr) => {
                info!("ReplicaState: {curr:b}, wait and retry");
                return false;
            },
        }
    }

    pub fn is_active(&self) -> bool {
        let state = self.inner.load(Ordering::SeqCst);
        if state == REPLICA_STATE_OPENED {
            return false;
        } else if state & (REPLICA_STATE_READ | REPLICA_STATE_WRITE | REPLICA_STATE_FLUSH | REPLICA_STATE_LOGGING) > 0 && state & REPLICA_STATE_OPENED == 0 {
            panic!("ReplicaState: invalid state, activities on replia but not opened");
        }
        true
    }
}

pub trait Replica: Sized + Send {
    #[allow(dead_code)]
    fn create(dev_path: &str, param1: usize, param2: usize) -> impl std::future::Future<Output = Self>;
    fn open(dev_path: &str) -> impl std::future::Future<Output = Self>;
    fn set_state_opened(&self);
    fn dup(&self) -> impl std::future::Future<Output = Self>;
    fn size(&self) -> u64;
    fn read(&self, offset: u64, buf: &mut [u8]) -> impl Future<Output = Result<usize>> + Send;
    fn write(&self, offset: u64, buf: &[u8]) -> impl Future<Output = Result<usize>> + Send;
    fn write_zero(&self, offset: u64, len: u64) -> impl Future<Output = Result<usize>>;
    fn flush(&self) -> impl Future<Output = Result<u64>>;
    fn close(&self) -> impl Future<Output = Result<u64>>;
    fn log_pending_io(&self, pending: Vec<PendingIo>, flush: bool) -> impl Future<Output = Result<u64>>;
    fn last_cno(&self) -> impl Future<Output = u64>;
    fn is_active(&self) -> bool;
    fn uuid(&self) -> u128;
}
