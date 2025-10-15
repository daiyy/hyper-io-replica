use std::io::Result;
use std::sync::Arc;
use smol::lock::RwLock;
use tokio::runtime;
use super::{Replica, ReplicaState};
use crate::replica::PendingIo;
use crate::utils;
use hyperfile::s3uri::S3Uri;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::config::{HyperFileRuntimeConfig, HyperFileMetaConfig};
use hyperfile::meta_format::BlockPtrFormat;

pub struct S3Replica<'a> {
    pub device_path: String,
    pub file: Arc<RwLock<Hyper<'a>>>,
    pub rt: Arc<runtime::Runtime>,
    pub stat: libc::stat,
    pub state: ReplicaState,
}

impl<'a: 'static> S3Replica<'a> {
    fn do_open(dev_path: &str) -> Self {
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            let (hyper, stat) = rt.block_on(async {
                let config = aws_config::load_from_env().await;
                let client = aws_sdk_s3::Client::new(&config);
                let flags = FileFlags::from(libc::O_RDWR);
                let _ = s3uri;
                let mut runtime_config = HyperFileRuntimeConfig::default_large();
                // disable hyper file internal max flush interval threshold
                runtime_config.data_cache_dirty_max_flush_interval = u64::MAX;
                let mut hyper = Hyper::fs_open_opt(&client, dev_path, flags, &runtime_config).await.expect("failed to open hyper file");
                let stat = hyper.fs_getattr().expect("unable to get hyper file stat");
                (hyper, stat)
            });
            return Self {
                device_path: dev_path.to_string(),
                file: Arc::new(RwLock::new(hyper)),
                rt: rt,
                stat: stat,
                state: ReplicaState::new(),
            };
        }
        panic!("invalid input device path {dev_path} for s3 replica");
    }
}

impl<'a: 'static> Replica for S3Replica<'a> {
    async fn create(dev_path: &str, dev_size: u64, meta_block_size: usize, data_block_size: usize) -> Self {
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            let (hyper, stat) = rt.block_on(async {
                let config = aws_config::load_from_env().await;
                let client = aws_sdk_s3::Client::new(&config);
                let _ = s3uri;

                let flags = FileFlags::rdwr();
                let mode = FileMode::default_file();
                let runtime_config = HyperFileRuntimeConfig::default();
                let default_meta_config = HyperFileMetaConfig::default();
                let meta_config = HyperFileMetaConfig::new(default_meta_config.root_size,
                    meta_block_size, data_block_size, BlockPtrFormat::MicroGroup,
                );
                let mut hyper = Hyper::fs_create_opt(&client, dev_path, flags, mode, &meta_config, &runtime_config).await.expect("failed to create replica hyper file");
                let _ = hyper.fs_truncate(dev_size as usize).await.expect("failed to extend size of replica hyper file");
                let stat = hyper.fs_getattr().expect("unable to get hyper file stat");
                (hyper, stat)
            });
            return Self {
                device_path: dev_path.to_string(),
                file: Arc::new(RwLock::new(hyper)),
                rt: rt,
                stat: stat,
                state: ReplicaState::new(),
            };
        }
        panic!("invalid input device path {dev_path} for s3 replica");
    }

    async fn open(dev_path: &str) -> Self {
        let path = dev_path.to_string();
        smol::unblock(move || Self::do_open(&path)).await
    }

    fn set_state_opened(&self) {
        self.state.set_opened();
    }

    async fn dup(&self) -> Self {
        Self {
            device_path: self.device_path.to_owned(),
            file: self.file.clone(),
            rt: self.rt.clone(),
            stat: self.stat.clone(),
            state: self.state.clone(),
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
        self.state.set_read();
        let res = smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_read(offset as usize, b).await
            })
        }).await;
        self.state.clear_read();
        res
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        if utils::is_all_zeros(buf) {
            match self.write_zero(offset, buf.len() as u64).await {
                Ok(len) => { return Ok(len); },
                Err(_) => { /* failback to physical write */ },
            }
        }
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        let b = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len()) };
        self.state.set_write();
        let res = smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_write(offset as usize, b).await
            })
        }).await;
        self.state.clear_write();
        res
    }

    async fn write_zero(&self, offset: u64, len: u64) -> Result<usize> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        self.state.set_write();
        let res = smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_write_zero(offset as usize, len as usize).await
            })
        }).await;
        self.state.clear_write();
        res
    }

    async fn flush(&self) -> Result<u64> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        self.state.set_flush();
        let res = smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_flush().await
            })
        }).await;
        self.state.clear_flush();
        res
    }

    async fn close(&self) -> Result<u64> {
        let rt = self.rt.clone();
        let hyper = self.file.clone();
        let segid = smol::unblock(move || {
            rt.block_on(async {
                let mut lock = hyper.write().await;
                lock.fs_release().await
            })
        }).await?;
        self.state.set_closed();
        Ok(segid)
    }

    async fn log_pending_io(&self, pending: Vec<PendingIo>, flush: bool) -> Result<u64> {
        let mut bytes = 0;
        self.state.set_logging();
        for io in pending.into_iter() {
            if io.size() == 0 {
                assert!(io.data_size() == 0);
                continue;
            }
            let offset = io.offset();
            let buf = io.as_ref();
            bytes += io.data_size();
            self.write(offset, buf).await.expect("unable to write replica deivce");
        }
        let segid = if flush {
            self.flush().await.expect("unable to flush replica deivce")
        } else {
            0
        };
        self.state.clear_logging();
        Ok(segid)
    }

    async fn last_cno(&self) -> u64 {
        self.file.read().await.fs_last_cno()
    }

    fn is_active(&self) -> bool {
        self.state.is_active()
    }

    fn uuid(&self) -> u128 {
        if let Some((_, uuid_str)) = self.device_path.rsplit_once('/') {
            if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                return uuid.as_u128();
            }
        };

        0
    }
}
