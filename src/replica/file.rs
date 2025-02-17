use std::io::Result;
use std::os::fd::AsRawFd;
use std::cell::RefCell;
use block_utils::Device;
use smol::fs::{File, OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use crate::utils;
use crate::pool::PendingIo;
use super::{Replica, ReplicaState};

#[derive(Debug)]
pub struct FileReplica {
    pub device_path: String,
    pub back_device: Device,
    pub file: RefCell<File>,
    pub state: ReplicaState,
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
            state: ReplicaState::new(),
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
            state: self.state.clone(),
        }
    }

    fn open(&self) {
        self.state.set_opened();
    }

    #[inline]
    fn size(&self) -> u64 {
        self.back_device.capacity
    }

    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.file.borrow_mut().seek(smol::io::SeekFrom::Start(offset)).await?;
        let len = buf.len();
        self.state.set_read();
        let res = self.file.borrow_mut().read_exact(buf).await;
        self.state.clear_read();
        res.map(|_| len)
    }

    async fn write(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        if utils::is_all_zeros(buf) {
            match self.write_zero(offset, len as u64).await {
                Ok(len) => { return Ok(len); },
                Err(_) => { /* failback to physical write */ },
            }
        }
        self.file.borrow_mut().seek(smol::io::SeekFrom::Start(offset)).await?;
        self.file.borrow_mut().write_all(buf).await?;
        Ok(len)
    }

    async fn write_zero(&self, offset: u64, len: u64) -> Result<usize> {
        let fd = self.file.borrow().as_raw_fd();
        smol::unblock(move || {
            let range = [offset, len];
            unsafe {
                ioctls::blkdiscard(fd, &range).map_err(to_io)
            }
        }).await?;
        Ok(len as usize)
    }

    async fn flush(&self) -> Result<u64> {
        self.file.borrow_mut().flush().await?;
        Ok(0)
    }

    async fn close(&self) -> Result<u64> {
        self.file.borrow_mut().close().await?;
        self.state.set_closed();
        Ok(0)
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
        0
    }

    fn is_active(&self) -> bool {
        self.state.is_active()
    }
}

fn to_io(err: nix::Error) -> std::io::Error {
    std::io::Error::from_raw_os_error(err as i32)
}

mod ioctls {
    use nix::{
        ioctl_none, ioctl_read_bad, ioctl_write_ptr_bad, request_code_none, request_code_read,
    };

    ioctl_none!(blkrrpart, 0x12, 95);
    ioctl_read_bad!(
        blkgetsize64,
        request_code_read!(0x12, 114, ::std::mem::size_of::<usize>()),
        u64
    );
    ioctl_read_bad!(
        blkdiscardzeros,
        request_code_none!(0x12, 124),
        ::std::os::raw::c_uint
    );
    ioctl_write_ptr_bad!(blkdiscard, request_code_none!(0x12, 119), [u64; 2]);
    ioctl_write_ptr_bad!(blkzeroout, request_code_none!(0x12, 127), [u64; 2]);
    ioctl_read_bad!(
        blksszget,
        request_code_none!(0x12, 104),
        ::std::os::raw::c_int
    );
}
