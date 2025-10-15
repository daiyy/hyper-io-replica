use std::io::Result;
use std::sync::Arc;
use tokio::runtime;
use tokio::sync::oneshot;
use log::debug;
use super::{Replica, ReplicaState};
use crate::replica::PendingIo;
use crate::utils;
use hyperfile_reactor::{TaskHandler, LocalSpawner};
use hyperfile::s3uri::S3Uri;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::handler::FileContext;
use hyperfile::file::mode::FileMode;
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::config::{HyperFileRuntimeConfig, HyperFileMetaConfig};
#[cfg(feature="pio-write-batch")]
use hyperfile::buffer::AlignedDataBlockWrapper;

pub struct S3Replica<'a> {
    pub device_path: String,
    pub rt: Arc<runtime::Runtime>,
    pub spawner: LocalSpawner<FileContext<'a>, Hyper<'a>>,
    pub handler: TaskHandler<FileContext<'a>>,
    pub stat: libc::stat,
    pub state: ReplicaState,
}

impl<'a: 'static> S3Replica<'a> {
    fn do_open(dev_path: &str) -> Self {
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            // a tokio mt runtime, the place spawn_read/spawn_write of hyperfile task will run
            let rt = Arc::new(
                runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            );
            let spawner = LocalSpawner::new_current();
            let (hyper, stat) = smol::block_on(async {
                rt.block_on(async {
                    let config = aws_config::load_from_env().await;
                    let client = aws_sdk_s3::Client::new(&config);
                    let flags = FileFlags::from(libc::O_RDWR);
                    let _ = s3uri;
                    let mut runtime_config = HyperFileRuntimeConfig::default_large();
                    // disable hyper file internal max flush interval threshold
                    runtime_config.data_cache_dirty_max_flush_interval = u64::MAX;
                    let hyper = Hyper::fs_open_opt(&client, dev_path, flags, &runtime_config).await.expect("failed to open hyper file");
                    let stat = hyper.fs_getattr().expect("unable to get hyper file stat");
                    (hyper, stat)
                })
            });
            let (tx, rx) = oneshot::channel();
            spawner.spawn(hyper, tx);
            let fh = smol::block_on(async {
                rt.block_on(async {
                    rx.await.expect("failed to get back file handler")
                })
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

    #[cfg(feature="pio-write-batch")]
    async fn do_write_batch(&self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize> {
        let (ctx, mut rx) = FileContext::new_write_aligned_batch(blocks);
        let rt = self.rt.clone();
        self.state.set_write();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.recv().await.expect("task channel closed")
            })
        }).await;
        self.state.clear_write();
        res
    }

    #[cfg(feature="pio-write-batch")]
    async fn write_batch(&self, pending: Vec<PendingIo>) -> Result<usize> {
        let start = std::time::Instant::now();
        let (mut stats_batch_count, mut stats_batch_bytes) = (0, 0);
        let (mut stats_solo_count, mut stats_solo_bytes) = (0, 0);
        let block_size = self.stat.st_blksize as u64;
        let mut total = 0;
        let mut batch = Vec::new();
        let mut list = pending.into_iter();
        loop {
            if let Some(io) = list.next() {

                if io.size() == 0 {
                    assert!(io.data_size() == 0);
                    continue;
                }
                let offset = io.offset();
                let size = io.data_size();
                let buf = io.as_ref();
                if (offset % block_size != 0) || (size as u64 % block_size != 0) {
                    // not a aligned block, use normal write path
                    if batch.len() > 0 {
                        // write all pending io before this non aligned block
                        let mut v = Vec::new();
                        v.append(&mut batch);
                        let bytes = self.do_write_batch(v).await.expect("unable to write batch replica deivce");
                        total += bytes;
                        stats_batch_count += 1;
                        stats_batch_bytes += bytes;
                    }
                    let bytes = self.write(offset, buf).await.expect("unable to write replica deivce");
                    total += bytes;
                    stats_solo_count += 1;
                    stats_solo_bytes += bytes;
                    continue;
                }
                // aligned block, split into block size chunks
                let mut start_offset = offset;
                for block_buf in buf.chunks(block_size as usize) {
                    let blk_idx = start_offset / block_size;
                    let block = if utils::is_all_zeros(block_buf) {
                        AlignedDataBlockWrapper::new(blk_idx, block_size as usize, true)
                    } else {
                        let b = AlignedDataBlockWrapper::new(blk_idx, block_size as usize, false);
                        b.as_mut_slice().copy_from_slice(block_buf);
                        b
                    };
                    batch.push(block);
                    start_offset += block_size;
                }
            } else {
                break;
            }
        }
        // send all remains
        if batch.len() > 0 {
            let bytes = self.do_write_batch(batch).await.expect("unable to write batch replica deivce");
            total += bytes;
            stats_solo_count += 1;
            stats_solo_bytes += bytes;
        }
        debug!("S3Replica - write batch - stats {{ batch_count: {}, batch_bytes: {}, solo_count: {}, solo_bytes: {} }} - cost: {:?}",
            stats_batch_count, stats_batch_bytes, stats_solo_count, stats_solo_bytes, start.elapsed());
        Ok(total)
    }
}

impl<'a: 'static> Replica for S3Replica<'a> {
    async fn create(dev_path: &str, dev_size: u64, meta_block_size: usize, data_block_size: usize) -> Self {
        if let Ok(s3uri) = S3Uri::parse(dev_path) {
            // a tokio mt runtime, the place spawn_read/spawn_write of hyperfile task will run
            let rt = Arc::new(
                runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            );
            let spawner = LocalSpawner::new_current();
            let (hyper, stat) = smol::block_on(async {
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
            let (tx, rx) = oneshot::channel();
            spawner.spawn(hyper, tx);
            let fh = smol::block_on(async {
                rt.block_on(async {
                    rx.await.expect("failed to get back file handler")
                })
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
            rt: self.rt.clone(),
            spawner: self.spawner.clone(),
            handler: self.handler.clone(),
            stat: self.stat.clone(),
            state: self.state.clone(),
        }
    }

    #[inline]
    fn size(&self) -> u64 {
        self.stat.st_size as u64
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let b = unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len()) };
        let (ctx, tx, mut rx) = FileContext::new_read(b, offset as usize, self.handler.clone());
        let rt = self.rt.clone();
        self.state.set_read();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.recv().await.expect("task channel closed")
            })
        }).await;
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
        let rt = self.rt.clone();
        self.state.set_write();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.recv().await.expect("task channel closed")
            })
        }).await;
        self.state.clear_write();
        drop(tx);
        res
    }

    async fn write_zero(&self, offset: u64, len: u64) -> Result<usize> {
        let (ctx, tx, mut rx) = FileContext::new_write_zero(offset as usize, len as usize, self.handler.clone());
        let rt = self.rt.clone();
        self.state.set_write();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.recv().await.expect("task channel closed")
            })
        }).await;
        self.state.clear_write();
        drop(tx);
        res
    }

    async fn flush(&self) -> Result<u64> {
        let (ctx, rx) = FileContext::new_flush(self.handler.clone());
        let rt = self.rt.clone();
        self.state.set_flush();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.await.expect("task channel closed")
            })
        }).await;
        self.state.clear_flush();
        res
    }

    async fn close(&self) -> Result<u64> {
        let (ctx, rx) = FileContext::new_release(self.handler.clone());
        let rt = self.rt.clone();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.await.expect("task channel closed")
            })
        }).await;
        self.state.set_closed();
        res
    }

    async fn log_pending_io(&self, pending: Vec<PendingIo>, flush: bool) -> Result<u64> {
        let mut _bytes = 0;
        self.state.set_logging();
        #[cfg(feature="pio-write-batch")]
        {
            _bytes = self.write_batch(pending).await?;
        }
        #[cfg(not(feature="pio-write-batch"))]
        for io in pending.into_iter() {
            if io.size() == 0 {
                assert!(io.data_size() == 0);
                continue;
            }
            let offset = io.offset();
            let buf = io.as_ref();
            _bytes += io.data_size();
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
        let (ctx, rx) = FileContext::new_last_cno();
        let rt = self.rt.clone();
        self.handler.send(ctx);
        let res = smol::unblock(move || {
            rt.block_on(async {
                rx.await.expect("task channel closed")
            })
        }).await;
        res
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
