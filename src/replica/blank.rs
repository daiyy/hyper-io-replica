use std::io::Result;
use crate::replica::{Replica, PendingIo};

#[allow(dead_code)]
pub struct BlankReplica {}

impl Replica for BlankReplica {
    async fn create(_dev_path: &str, _dev_size: u64, _param1: usize, _param2: usize) -> Self {
        Self {}
    }

    async fn open(_dev_path: &str) -> Self {
        Self {}
    }

    fn set_state_opened(&self) {
        todo!();
    }

    async fn dup(&self) -> Self {
        todo!();
    }

    fn size(&self) -> u64 {
        todo!();
    }

    async fn read(&self, _offset: u64, _buf: &mut [u8]) -> Result<usize> {
        todo!();
    }

    async fn write(&self, _offset: u64, _buf: &[u8]) -> Result<usize> {
        todo!();
    }

    async fn write_zero(&self, _offset: u64, _len: u64) -> Result<usize> {
        todo!();
    }

    async fn flush(&self) -> Result<u64> {
        todo!();
    }

    async fn close(&self) -> Result<u64> {
        todo!();
    }

    async fn log_pending_io(&self, _pending: Vec<PendingIo>, _flush: bool) -> Result<u64> {
        todo!();
    }

    async fn last_cno(&self) -> u64 {
        todo!();
    }

    fn is_active(&self) -> bool {
        todo!();
    }

    fn uuid(&self) -> u128 {
        todo!();
    }
}
