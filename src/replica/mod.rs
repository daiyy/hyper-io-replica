use std::io::Result;
use std::future::Future;

mod file;

pub trait ReplicaOp: Sized {
    fn new(dev_path: &str) -> impl std::future::Future<Output = Self>;
    fn size(&self) -> u64;
    fn read(&self, offset: u64, buf: &mut [u8]) -> impl Future<Output = Result<usize>>;
    fn write(&self, offset: u64, buf: &[u8]) -> impl Future<Output = Result<usize>>;
    fn flush(&self) -> impl Future<Output = Result<u64>>;
    fn close(&self) -> impl Future<Output = Result<u64>>;
}
