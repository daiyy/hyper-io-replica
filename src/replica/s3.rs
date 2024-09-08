use std::io::Result;
use super::Replica;
use crate::replica::PendingIo;

#[derive(Debug)]
pub struct S3Replica {
    pub device_path: String,
}

impl Replica for S3Replica {
    async fn new(dev_path: &str) -> Self {
        todo!();
    }

    async fn dup(&self) -> Self {
        todo!();
    }

    #[inline]
    fn size(&self) -> u64 {
        todo!();
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        todo!();
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        todo!();
    }

    async fn flush(&self) -> Result<u64> {
        Ok(0)
    }

    async fn close(&self) -> Result<u64> {
        Ok(0)
    }

    async fn log_pending_io(&self, pending: Vec<PendingIo>) -> Result<()> {
        Ok(())
    }
}
