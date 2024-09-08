use std::io::Result;
use std::sync::Arc;
use std::pin::Pin;
use smol::lock::RwLock;
use tokio::runtime;
use super::Replica;
use crate::replica::PendingIo;
use s3_hyperfile::s3uri::S3Uri;
use s3_hyperfile::file::hyper::Hyper;
use s3_hyperfile::file::flags::FileFlags;

pub struct S3Replica<'a> {
    pub device_path: String,
    pub file: Arc<RwLock<Hyper<'a>>>,
    pub rt: Arc<runtime::Runtime>,
}

impl<'a: 'static> S3Replica<'a> {
    pub fn init(dev_path: &str) -> Self {
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            let hyper = rt.block_on(async {
                let config = aws_config::load_from_env().await;
                let client = aws_sdk_s3::Client::new(&config);
                let flags = FileFlags::from(libc::O_RDWR);
                Hyper::fs_open(&client, s3uri.bucket, s3uri.key, flags).await.expect("failed to open hyper file")
            });
            return Self {
                device_path: dev_path.to_string(),
                file: Arc::new(RwLock::new(hyper)),
                rt: rt,
            };
        }
        panic!("invalid input device path {dev_path} for s3 replica");
    }
}

impl<'a: 'static> Replica for S3Replica<'a> {
    async fn new(dev_path: &str) -> Self {
        let path = dev_path.to_string();
        smol::unblock(move || Self::init(&path)).await
    }

    async fn dup(&self) -> Self {
        todo!();
    }

    #[inline]
    fn size(&self) -> u64 {
        let stat = self.file.read_arc_blocking().fs_getattr().unwrap();
        stat.st_size as u64
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
