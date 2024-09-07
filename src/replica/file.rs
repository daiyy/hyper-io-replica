use std::io::Result;
use block_utils::Device;
use smol::fs::{OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use super::ReplicaOp;

#[derive(Clone, Debug)]
pub struct FileReplica {
    pub device_path: String,
    pub back_device: Device,
}

impl ReplicaOp for FileReplica {
    async fn new(dev_path: &str) -> Self {
        let device = block_utils::get_device_info(dev_path)
            .unwrap_or_else(|_| panic!("failed to get replica device path {dev_path}"));
        Self {
            device_path: dev_path.to_string(),
            back_device: device,
        }
    }

    #[inline]
    fn size(&self) -> u64 {
        self.back_device.capacity
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(self.device_path.as_str())
            .await?;
        file.seek(smol::io::SeekFrom::Start(offset)).await?;
        let len = buf.len();
        file.read_exact(buf).await?;
        Ok(len)
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let mut file = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(self.device_path.as_str())
            .await?;
        file.seek(smol::io::SeekFrom::Start(offset)).await?;
        let len = buf.len();
        file.write_all(buf).await?;
        Ok(len)
    }

    async fn flush(&self) -> Result<u64> {
        Ok(0)
    }

    async fn close(&self) -> Result<u64> {
        Ok(0)
    }
}
