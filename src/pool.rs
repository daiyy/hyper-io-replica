use std::fmt;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;
use smol::channel;
use log::{info, debug};

pub(crate) struct PendingIo {
    flags: u32,
    size: u32,
    offset: u64,
    data: IoBuf<u8>,
}

impl fmt::Debug for PendingIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PendingIo {{ flags: {}, offset: {}, size: {} }}", self.flags, self.offset, self.size)
    }
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

pub(crate) struct LocalPendingBlocksPool {
    blocks: Vec<PendingIo>,
    // size in bytes
    max_capacity: usize,
    current_capacity: usize,
    tx: channel::Sender<Vec<PendingIo>>,
}

impl fmt::Debug for LocalPendingBlocksPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalPendingBlocksPool {{ blocks: {}, available capacity: {} }}", self.blocks.len(), self.avail_capacity())
    }
}

impl LocalPendingBlocksPool {
    pub(crate) fn new(max_capacity: usize, tx: channel::Sender<Vec<PendingIo>>) -> Self {
        Self {
            blocks: Vec::new(),
            max_capacity: max_capacity,
            current_capacity: 0,
            tx: tx,
        }
    }

    pub(crate) fn get_count(&self) -> usize {
        self.blocks.len()
    }

    pub(crate) fn avail_capacity(&self) -> usize {
        if self.max_capacity >= self.current_capacity {
            return self.max_capacity - self.current_capacity;
        }
        0
    }

    pub(crate) fn append(&mut self, pio: PendingIo) {
        self.blocks.push(pio);
    }

    // push pending io from local to central
    //
    // in 3 cases:
    // 1. no enough avail mem in local pool
    // 2. 1s timeout
    // 3. a flush op completed
    pub(crate) fn propagate(&mut self) {
        if self.blocks.len() == 0 {
            return;
        }
        let mut v = Vec::new();
        std::mem::swap(&mut v, &mut self.blocks);
        let _ = self.tx.send_blocking(v);
        self.current_capacity = 0;
    }
}

pub(crate) struct TgtPendingBlocksPool {
    rx: channel::Receiver<Vec<PendingIo>>,
    tx: channel::Sender<Vec<PendingIo>>,
}

impl TgtPendingBlocksPool {
    pub(crate) fn new() -> Self {
        let (tx, rx) = channel::unbounded();
        Self {
            rx: rx,
            tx: tx,
        }
    }

    pub(crate) fn get_tx_chan(&self) -> channel::Sender<Vec<PendingIo>> {
        self.tx.clone()
    }

    pub(crate) async fn main_loop(self) {
        info!("TgtPendingBlocksPool started");
        while let Ok(v) = self.rx.recv().await {
            debug!("TgtPendingBlocksPool receved {} size of pending io vec", v.len());
        }
        info!("TgtPendingBlocksPool quit");
    }

    pub(crate) fn start(self) -> std::thread::JoinHandle<()> {
        std::thread::spawn(|| {
            smol::block_on(async move {
                self.main_loop().await;
            });
        })
    }
}
