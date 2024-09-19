use std::io::Result;
use std::sync::Arc;
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
        Self {
            device_path: self.device_path.to_owned(),
            file: self.file.clone(),
            rt: self.rt.clone(),
        }
    }

    #[inline]
    fn size(&self) -> u64 {
        let stat = self.file.read_arc_blocking().fs_getattr().unwrap();
        stat.st_size as u64
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        let b = unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len()) };
        smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_read(b, offset as usize).await
            })
        }).await
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        let b = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len()) };
        smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_write(b, offset as usize).await
            })
        }).await
    }

    async fn write_zero(&self, offset: u64, len: u64) -> Result<usize> {
        todo!();
    }

    async fn flush(&self) -> Result<u64> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_flush().await
            })
        }).await
        // TODO: return cno
        Ok(0)
    }

    async fn close(&self) -> Result<u64> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_release(false).await
            })
        }).await
        // TODO: return cno
        Ok(0)
    }

    async fn log_pending_io(&self, pending: Vec<PendingIo>) -> Result<()> {
        for io in pending.into_iter() {
            if io.size() == 0 {
                assert!(io.data_size() == 0);
                continue;
            }
            let offset = io.offset();
            let buf = io.as_ref();
            self.write(offset, buf).await.expect("unable to write replica deivce");
        }
        self.flush().await.expect("unable to flush replica deivce");
        Ok(())
    }
}
