use std::io::Result;
use std::future::Future;
use crate::pool::PendingIo;

pub(crate) mod blank;
pub(crate) mod file;
#[cfg(feature="blocking")]
pub(crate) mod s3;
#[cfg(feature="reactor")]
pub(crate) mod s3_reactor;

pub trait Replica: Sized + Send {
    fn new(dev_path: &str) -> impl std::future::Future<Output = Self>;
    fn dup(&self) -> impl std::future::Future<Output = Self>;
    fn size(&self) -> u64;
    fn read(&self, offset: u64, buf: &mut [u8]) -> impl Future<Output = Result<usize>>;
    fn write(&self, offset: u64, buf: &[u8]) -> impl Future<Output = Result<usize>>;
    fn write_zero(&self, offset: u64, len: u64) -> impl Future<Output = Result<usize>>;
    fn flush(&self) -> impl Future<Output = Result<u64>>;
    fn close(&self) -> impl Future<Output = Result<u64>>;
    fn log_pending_io(&self, pending: Vec<PendingIo>, flush: bool) -> impl Future<Output = Result<u64>>;
    fn last_cno(&self) -> impl Future<Output = u64>;
}
