use std::io::Result;
use std::sync::Arc;
use tokio::runtime;
use tokio::sync::oneshot;
use super::{Replica, ReplicaState};
use crate::replica::PendingIo;
use crate::utils;
use reactor::{TaskHandler, LocalSpawner};
use hyperfile::s3uri::S3Uri;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::handler::FileContext;

pub struct S3Replica<'a> {
    pub device_path: String,
    pub rt: Arc<runtime::Runtime>,
    pub spawner: LocalSpawner<FileContext<'a>, Hyper<'a>>,
    pub handler: TaskHandler<FileContext<'a>>,
    pub stat: libc::stat,
    pub state: ReplicaState,
}

impl<'a: 'static> S3Replica<'a> {
    pub fn init(dev_path: &str) -> Self {
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            let rt = Arc::new(
                runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            );
            let spawner = LocalSpawner::new(Some(rt.clone()));
            let (hyper, stat) = rt.block_on(async {
                let config = aws_config::load_from_env().await;
                let client = aws_sdk_s3::Client::new(&config);
                let flags = FileFlags::from(libc::O_RDWR);
                let _ = s3uri;
                let hyper = Hyper::fs_open(&client, dev_path, flags).await.expect("failed to open hyper file");
                let stat = hyper.fs_getattr().expect("unable to get hyper file stat");
                (hyper, stat)
            });
            let (tx, rx) = oneshot::channel();
            spawner.spawn(hyper, tx);
            let fh = rt.block_on(async {
                rx.await.expect("failed to get back file handler")
            });
            return Self {
                device_path: dev_path.to_string(),
                rt: rt,
                spawner: spawner,
                handler: fh,
                stat: stat,
                state: ReplicaState::new(),
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
            rt: self.rt.clone(),
            spawner: self.spawner.clone(),
            handler: self.handler.clone(),
            stat: self.stat.clone(),
            state: self.state.clone(),
        }
    }

    fn open(&self) {
        self.state.set_opened();
    }

    #[inline]
    fn size(&self) -> u64 {
        self.stat.st_size as u64
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let b = unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len()) };
        let (ctx, tx, mut rx) = FileContext::new_read(b, offset as usize);
        self.state.set_read();
        self.handler.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
        self.state.clear_read();
        drop(tx);
        res
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        if utils::is_all_zeros(buf) {
            match self.write_zero(offset, buf.len() as u64).await {
                Ok(len) => { return Ok(len); },
                Err(_) => { /* failback to physical write */ },
            }
        }
        let b = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len()) };
        let (ctx, tx, mut rx) = FileContext::new_write(b, offset as usize, self.handler.clone());
        self.state.set_read();
        self.handler.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
        self.state.clear_read();
        drop(tx);
        res
    }

    async fn write_zero(&self, offset: u64, len: u64) -> Result<usize> {
        let (ctx, tx, mut rx) = FileContext::new_write_zero(offset as usize, len as usize, self.handler.clone());
        self.state.set_write();
        self.handler.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
        self.state.clear_write();
        drop(tx);
        res
    }

    async fn flush(&self) -> Result<u64> {
        let (ctx, rx) = FileContext::new_flush();
        self.handler.send(ctx);
        let res = rx.await.expect("task channel closed");
        res
    }

    async fn close(&self) -> Result<u64> {
        let (ctx, rx) = FileContext::new_release();
        self.handler.send(ctx);
        let res = rx.await.expect("task channel closed");
        self.state.set_closed();
        res
    }

    async fn log_pending_io(&self, pending: Vec<PendingIo>, flush: bool) -> Result<u64> {
        let mut bytes = 0;
        for io in pending.into_iter() {
            if io.size() == 0 {
                assert!(io.data_size() == 0);
                continue;
            }
            let offset = io.offset();
            let buf = io.as_ref();
            bytes += io.data_size();
            self.state.set_write();
            self.write(offset, buf).await.expect("unable to write replica deivce");
        }
        let segid = if flush {
            self.state.set_flush();
            self.flush().await.expect("unable to flush replica deivce")
        } else {
            0
        };
        if bytes > 0 { self.state.clear_write() }
        if flush { self.state.clear_flush() }
        Ok(segid)
    }

    async fn last_cno(&self) -> u64 {
        let (ctx, rx) = FileContext::new_last_cno();
        self.handler.send(ctx);
        rx.await.expect("task channel closed")
    }

    fn is_active(&self) -> bool {
        self.state.is_active()
    }
}
