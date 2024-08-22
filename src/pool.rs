use std::fmt;
use std::io::Result;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;

pub(crate) struct PendingIo {
    flags: u32,
    size: u32,
    offset: u64,
    data: IoBuf<u8>,
}

impl PendingIo {
    pub(crate) fn from_iodesc(iod: &ublksrv_io_desc) -> Self {
        let size = iod.nr_sectors << 9;
        let data = IoBuf::new(size as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(iod.addr as *const u8, data.as_mut_ptr(), size as usize);
        }
        Self {
            flags: iod.op_flags,
            size: size,
            offset: iod.start_sector << 9,
            data: data,
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.size as usize
    }
}

pub(crate) struct PendingBlocksPool {
    blocks: Vec<PendingIo>,
    // size in bytes
    max_capacity: usize,
    current_capacity: usize,
}

impl fmt::Debug for PendingBlocksPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PendingBlocksPool {{ blocks: {}, available capacity: {} }}", self.blocks.len(), self.avail_capacity())
    }
}

impl PendingBlocksPool {
    pub(crate) fn new(max_capacity: usize) -> Self {
        Self {
            blocks: Vec::new(),
            max_capacity: max_capacity,
            current_capacity: 0,
        }
    }

    pub(crate) fn avail_capacity(&self) -> usize {
        if self.max_capacity >= self.current_capacity {
            return self.max_capacity - self.current_capacity;
        }
        0
    }

    pub(crate) fn append(&mut self, pio: PendingIo) {
        self.current_capacity += pio.size();
        self.blocks.push(pio);
    }

    // flush dirty blocks to hyper
    pub fn flush_hyper(&mut self) -> Vec<PendingIo> {
        // sort
        // merge
        // output
        return Vec::new();
    }

    // flush checkpoint to primary
    pub fn flush_checkpoint(&mut self, cno: u64) {
        // collect dirty
        // Serialization
        // write to superblock on primary
    }
}
