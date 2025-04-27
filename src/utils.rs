use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{fence, Ordering};
use ilog::IntLog;
use libublk::ctrl::UblkCtrl;
use libublk::sys::*;
use shared_memory::*;

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

/// Write device ID into shared memory, so that parent process can
/// know this ID info
///
/// The 1st 4 char is : 'U' 'B' 'L' 'K', then follows the 4
/// ID chars which is encoded by hex.
pub(crate) fn rublk_write_id_into_shm(shm_id: &String, id: u32) {
    log::info!("shm_id {} id {}", shm_id, id);
    match ShmemConf::new().os_id(shm_id).size(4096).open() {
        Ok(mut shmem) => {
            let s: &mut [u8] = unsafe { shmem.as_slice_mut() };

            let id_str = format!("{:04x}", id);
            let mut i = 4;
            for c in id_str.as_bytes() {
                s[i] = *c;
                i += 1;
            }

            // order the two WRITEs
            fence(Ordering::Release);

            s[0] = b'U';
            s[1] = b'B';
            s[2] = b'L';
            s[3] = b'K';
        }
        Err(e) => println!("write id open failed {} {}", shm_id, e),
    }
}

pub(crate) fn rublk_prep_dump_dev(shm_id: Option<String>, fg: bool, ctrl: &UblkCtrl) {
    if !fg {
        if let Some(shm) = shm_id {
            rublk_write_id_into_shm(&shm, ctrl.dev_info().dev_id);
        }
    } else {
        ctrl.dump();
    }
}

#[allow(dead_code)]
pub(crate) fn iod_fmt(iod: &ublksrv_io_desc, region_shift: u32) -> String {
    let offset = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;

    let op = iod.op_flags & 0xff;
    let mut o = Vec::new();
    match op {
        UBLK_IO_OP_READ => { o.push("Read"); },
        UBLK_IO_OP_FLUSH => { o.push("Flush"); },
        UBLK_IO_OP_WRITE => { o.push("Write"); },
        UBLK_IO_OP_DISCARD => { o.push("Discard"); },
        UBLK_IO_OP_ZONE_OPEN => { o.push("ZoneOpen"); },
        UBLK_IO_OP_WRITE_SAME => { o.push("WriteSame"); },
        UBLK_IO_OP_ZONE_CLOSE => { o.push("ZoneClose"); },
        UBLK_IO_OP_ZONE_RESET => { o.push("ZoneReset"); },
        UBLK_IO_OP_ZONE_APPEND => { o.push("ZoneAppend"); },
        UBLK_IO_OP_ZONE_FINISH => { o.push("ZoneFinish"); },
        UBLK_IO_OP_REPORT_ZONES => { o.push("ReportZones"); },
        UBLK_IO_OP_WRITE_ZEROES => { o.push("WriteZeroes"); },
        UBLK_IO_OP_ZONE_RESET_ALL => { o.push("ZoneResetAll"); },
        _ => { o.push("Unkown"); },
    }
	let o_str = o.join(" | ");

    let mut f = Vec::new();
    let flags = iod.op_flags & 0xfff00;
    if flags & UBLK_IO_F_FAILFAST_DEV > 0 { f.push("FailFastDev"); }
    if flags & UBLK_IO_F_FAILFAST_TRANSPORT > 0 { f.push("FailFastTransport"); }
    if flags & UBLK_IO_F_FAILFAST_DRIVER > 0 { f.push("FailFastDriver"); }
    if flags & UBLK_IO_F_META > 0 { f.push("Meta"); }
    if flags & UBLK_IO_F_FUA > 0 { f.push("Fua"); }
    if flags & UBLK_IO_F_NOUNMAP > 0 { f.push("NoUnmap"); }
    if flags & UBLK_IO_F_SWAP > 0 { f.push("Swap"); }
    let f_str = f.join(" | ");
    let f_str = if f_str == "" {
        "-"
    } else {
        &f_str
    };

    let region_id = offset / (1 << region_shift);
    format!("IoDesc {{ op: {}, flag: {}, region: {}, offset: {}, bytes: {}, op_flags: {:x}}}", o_str, f_str, region_id, offset, bytes, iod.op_flags)
}
