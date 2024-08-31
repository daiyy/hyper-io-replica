use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;
use smol::channel;
use smol::LocalExecutor;
use smol::fs::{OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncSeekExt, AsyncWriteExt};
use log::{info, debug};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::mgmt::CommandChannel;

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
    replica_path: String,
    rx: channel::Receiver<Vec<PendingIo>>,
    tx: channel::Sender<Vec<PendingIo>>,
    pending_queue: Vec<PendingIo>,
    pending_bytes: usize,
    max_capacity: usize,
}

impl TgtPendingBlocksPool {
    pub(crate) fn new(max_capacity: usize, replica_path: &str) -> Self {
        let (tx, rx) = channel::unbounded();
        Self {
            rx: rx,
            tx: tx,
            pending_queue: Vec::new(),
            pending_bytes: 0,
            max_capacity: max_capacity,
            replica_path: replica_path.to_string(),
        }
    }

    pub(crate) fn get_tx_chan(&self) -> channel::Sender<Vec<PendingIo>> {
        self.tx.clone()
    }

    async fn write_to_replica(replica_path: String, pending: Vec<PendingIo>) {

        let mut replica_dev = OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(replica_path)
            .await.expect("unable to open replica device");

        for io in pending.into_iter() {
            if io.size == 0 {
                assert!(io.data.len() == 0);
                continue;
            }
            let offset = io.offset;
            let buf = &(io.data);
            replica_dev.seek(smol::io::SeekFrom::Start(offset)).await.expect("unable to seek replica device");
            replica_dev.write_all(buf).await.expect("unable to write replica deivce");
            replica_dev.flush().await.expect("unable to flush replica deivce");
        }
    }

    pub(crate) async fn main_loop(pool: Rc<RefCell<Self>>, state: Rc<GlobalTgtState>, region: Rc<Region>, _recover: Rc<RecoverCtrl>) {
        info!("TgtPendingBlocksPool started with:");
        info!("  - state {:?}", state);
        info!("  - region {:?}", region);
        let rx = pool.borrow().rx.clone();
        while let Ok(mut v) = rx.recv().await {
            let total_bytes: usize = v.iter().map(|pio| pio.size()).sum();
            pool.borrow_mut().pending_bytes += total_bytes;
            pool.borrow_mut().pending_queue.append(&mut v);
            if pool.borrow().pending_bytes >= pool.borrow().max_capacity {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes exceed max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();

                let replica_path = pool.borrow().replica_path.to_string();
                Self::write_to_replica(replica_path, pending).await;

                state.set_logging_disable();

                pool.borrow_mut().pending_queue = Vec::new();
                pool.borrow_mut().pending_bytes = 0;
            } else if pool.borrow().pending_bytes >= pool.borrow().max_capacity / 2 {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes exceed 1/2 max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();

                let replica_path = pool.borrow().replica_path.to_string();
                Self::write_to_replica(replica_path, pending).await;

                pool.borrow_mut().pending_queue = Vec::new();
                pool.borrow_mut().pending_bytes = 0;
            }
        }
        info!("TgtPendingBlocksPool quit");
    }

    pub(crate) async fn periodic(pool: Rc<RefCell<Self>>) {
        loop {
            smol::Timer::after(std::time::Duration::from_secs(1)).await;
            let pending_bytes = pool.borrow().pending_bytes;
            let pending_queue_len = pool.borrow().pending_queue.len();
            if pending_bytes > 0 {
                debug!("TgtPendingBlocksPool - {} of pending IO, total {} bytes, periodic flush",
                    pending_queue_len, pending_bytes);
                let pending = pool.borrow_mut().pending_queue.drain(..).collect();

                let replica_path = pool.borrow().replica_path.to_string();
                Self::write_to_replica(replica_path, pending).await;

                pool.borrow_mut().pending_queue = Vec::new();
                pool.borrow_mut().pending_bytes = 0;
            }
        }
    }

    pub(crate) fn start(self, state: GlobalTgtState, region: Region, recover: RecoverCtrl) -> std::thread::JoinHandle<()> {
        std::thread::spawn(|| {
            let mut f_vec = Vec::new();
            let exec = LocalExecutor::new();

            let rc_pool = Rc::new(RefCell::new(self));
            let rc_state = Rc::new(state);
            let rc_region = Rc::new(region);
            let rc_recover = Rc::new(recover);

            let c_pool = rc_pool.clone();
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            f_vec.push(exec.spawn(async move {
                Self::main_loop(c_pool, c_state, c_region, c_recover).await;
            }));

            let c_pool = rc_pool.clone();
            f_vec.push(exec.spawn(async move {
                Self::periodic(c_pool).await;
            }));

            let c_recover = rc_recover.clone();
            f_vec.push(exec.spawn(async move {
                c_recover.main_loop().await;
            }));

            let cmd_chan = CommandChannel::new();
            let c_state = rc_state.clone();
            let c_region = rc_region.clone();
            let c_recover = rc_recover.clone();
            f_vec.push(exec.spawn(async move {
                cmd_chan.main_handler(c_state, c_region, c_recover).await;
            }));

            smol::block_on(async { loop { exec.tick().await }});
        })
    }
}
