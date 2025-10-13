use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use ilog::IntLog;
use libublk::ctrl::UblkCtrl;
use libublk::sys::*;

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

nix::ioctl_read!(ioctl_blkgetsize64, BLK_IOCTL_TYPE, BLKGETSIZE64_NR, u64);
nix::ioctl_read_bad!(
    ioctl_blksszget,
    nix::request_code_none!(BLK_IOCTL_TYPE, BLKSSZGET_NR),
    i32
);
nix::ioctl_read_bad!(
    ioctl_blkpbszget,
    nix::request_code_none!(BLK_IOCTL_TYPE, BLKPBSZGET_NR),
    u32
);

pub(crate) struct DevIdComm {
    efd: std::os::fd::OwnedFd,
    dump: bool,
}

impl DevIdComm {
    pub fn new(dump: bool) -> anyhow::Result<DevIdComm> {
        let fd = nix::sys::eventfd::EventFd::from_value_and_flags(
            0,
            nix::sys::eventfd::EfdFlags::empty(),
        )?
        .into();

        Ok(DevIdComm { efd: fd, dump })
    }

    pub(crate) fn write_failure(&self) -> anyhow::Result<i32> {
        let id = i64::MAX;
        let bytes = id.to_le_bytes();

        match nix::unistd::write(&self.efd, &bytes) {
            Ok(_) => Ok(0),
            _ => Err(anyhow::anyhow!("fail to write failure to eventfd")),
        }
    }

    pub(crate) fn write_dev_id(&self, dev_id: u32) -> anyhow::Result<i32> {
        // Can't write 0 to eventfd file, otherwise the read() side may
        // not be waken up
        let id = (dev_id + 1) as i64;
        let bytes = id.to_le_bytes();

        match nix::unistd::write(&self.efd, &bytes) {
            Ok(_) => Ok(0),
            _ => Err(anyhow::anyhow!("fail to write dev_id to eventfd")),
        }
    }

    pub(crate) fn read_dev_id(&self) -> anyhow::Result<i32> {
        let mut buffer = [0; 8];

        let bytes_read = nix::unistd::read(&self.efd, &mut buffer)?;
        if bytes_read == 0 {
            return Err(anyhow::anyhow!("fail to read dev_id from eventfd"));
        }
        let ret = i64::from_le_bytes(buffer);
        if ret == i64::MAX {
            return Err(anyhow::anyhow!("Fail to start ublk daemon"));
        }

        Ok((ret - 1) as i32)
    }

    pub(crate) fn send_dev_id(&self, id: u32) -> anyhow::Result<()> {
        if self.dump {
            UblkCtrl::new_simple(id as i32).unwrap().dump();
        } else {
            self.write_dev_id(id).expect("Fail to write efd");
        }
        Ok(())
    }

    pub(crate) fn recieve_dev_id(&self) -> anyhow::Result<i32> {
        let id = self.read_dev_id()?;
        Ok(id)
    }
}

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
