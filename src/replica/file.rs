use std::io::Result;
use std::cell::RefCell;
use block_utils::Device;
use smol::fs::{File, OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use crate::pool::PendingIo;
use super::Replica;

#[derive(Debug)]
pub struct FileReplica {
    pub device_path: String,
    pub back_device: Device,
    pub file: RefCell<File>,
}

impl Replica for FileReplica {
    async fn new(dev_path: &str) -> Self {
        let device = block_utils::get_device_info(dev_path)
            .unwrap_or_else(|_| panic!("failed to get replica device path {dev_path}"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev_path)
            .await
            .unwrap_or_else(|_| panic!("failed to open {dev_path}"));
        Self {
            device_path: dev_path.to_string(),
            back_device: device,
            file: RefCell::new(file),
        }
    }

    async fn dup(&self) -> Self {
        // create a new instance of file
        let dev_path = self.device_path.as_str();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev_path)
            .await
            .unwrap_or_else(|_| panic!("failed to open {dev_path}"));
        Self {
            device_path: self.device_path.clone(),
            back_device: self.back_device.clone(),
            file: RefCell::new(file),
        }
    }

    #[inline]
    fn size(&self) -> u64 {
        self.back_device.capacity
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.file.borrow_mut().seek(smol::io::SeekFrom::Start(offset)).await?;
        let len = buf.len();
        self.file.borrow_mut().read_exact(buf).await?;
        Ok(len)
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        self.file.borrow_mut().seek(smol::io::SeekFrom::Start(offset)).await?;
        let len = buf.len();
        self.file.borrow_mut().write_all(buf).await?;
        Ok(len)
    }

    async fn flush(&self) -> Result<u64> {
        self.file.borrow_mut().flush().await?;
        Ok(0)
    }

    async fn close(&self) -> Result<u64> {
        self.file.borrow_mut().close().await?;
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
