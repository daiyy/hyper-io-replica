use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use ilog::IntLog;

pub(crate) fn is_all_zeros(buf: &[u8]) -> bool {
    let (prefix, aligned, suffix) = unsafe { buf.align_to::<u128>() };

    prefix.iter().all(|&x| x == 0)
        && suffix.iter().all(|&x| x == 0)
        && aligned.iter().all(|&x| x == 0)
}

// Generate ioctl function
const BLK_IOCTL_TYPE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_NR: u8 = 114;
const BLKSSZGET_NR: u8 = 104;
const BLKPBSZGET_NR: u8 = 123;

ioctl_read!(ioctl_blkgetsize64, BLK_IOCTL_TYPE, BLKGETSIZE64_NR, u64);
ioctl_read_bad!(
    ioctl_blksszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKSSZGET_NR),
    i32
);
ioctl_read_bad!(
    ioctl_blkpbszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKPBSZGET_NR),
    u32
);

// return (device size in bytes, bit shift of sector size in bytes, bit shift of physical block (sector) size)
pub(crate) fn ublk_file_size(f: &std::fs::File) -> anyhow::Result<(u64, u8, u8)> {
    if let Ok(meta) = f.metadata() {
        if meta.file_type().is_block_device() {
            let fd = f.as_raw_fd();
            let mut cap = 0_u64;
            let mut ssz = 0_i32;
            let mut pbsz = 0_u32;

            unsafe {
                let cap_ptr = &mut cap as *mut u64;
                let ssz_ptr = &mut ssz as *mut i32;
                let pbsz_ptr = &mut pbsz as *mut u32;

                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
                ioctl_blksszget(fd, ssz_ptr).unwrap();
                ioctl_blkpbszget(fd, pbsz_ptr).unwrap();
            }

            Ok((cap, ssz.log2() as u8, pbsz.log2() as u8))
        } else if meta.file_type().is_file() {
            let m = f.metadata().unwrap();
            Ok((
                m.len(),
                m.st_blksize().log2() as u8,
                m.st_blksize().log2() as u8,
            ))
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}
