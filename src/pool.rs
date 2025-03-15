use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::BTreeMap;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;
use smol::channel;
use log::{info, debug};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::replica::Replica;
use crate::device::MetaDeviceDesc;
use crate::task::{TaskState, TaskId};
use crate::seq::IncrSeq;
use crate::stats::PoolStats;

pub(crate) enum PendingIo {
    Write(WriteIo),
    Flush(FlushIo),
}

pub(crate) struct WriteIo {
    flags: u32,
    size: u32,
    offset: u64,
    data: IoBuf<u8>,
    seq: u64,
}

pub(crate) struct FlushIo {
    flags: u32,
    size: u32,
    offset: u64,
    seq: u64,
}

impl fmt::Debug for PendingIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(io) => write!(f, "PendingIo {{ op: Write, flags: {}, offset: {}, size: {} }}", io.flags, io.offset, io.size),
            Self::Flush(io) => write!(f, "PendingIo {{ op: Flush, flags: {}, offset: {}, size: {} }}", io.flags, io.offset, io.size),
        }
    }
}

impl PendingIo {
    pub(crate) fn from_iodesc(iod: &ublksrv_io_desc, seq: u64) -> Self {
        let size = iod.nr_sectors << 9;
        let offset = iod.start_sector << 9;
        match iod.op_flags & 0xff {
            libublk::sys::UBLK_IO_OP_WRITE => {},
            libublk::sys::UBLK_IO_OP_FLUSH => {
                return Self::Flush(FlushIo { flags: iod.op_flags, size: size, offset: offset, seq: seq,});
            },
            _ => { panic!("op {:#02x} not implement", iod.op_flags & 0xff); },
        }

        // op WRITE
        let data = IoBuf::new(size as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(iod.addr as *const u8, data.as_mut_ptr(), size as usize);
        }
        Self::Write(WriteIo {
            flags: iod.op_flags,
            size: size,
            offset: offset,
            data: data,
            seq: seq,
        })
    }

    #[inline]
    pub(crate) fn offset(&self) -> u64 {
        match self {
            Self::Write(io) => io.offset,
            Self::Flush(io) => io.offset,
        }
    }

    #[inline]
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Write(io) => io.size as usize,
            Self::Flush(io) => io.size as usize,
        }
    }

    #[inline]
    pub(crate) fn data_size(&self) -> usize {
        match self {
            Self::Write(io) => io.data.len(),
            Self::Flush(_) => 0,
        }
    }

    #[inline]
    pub(crate) fn as_ref(&self) -> &[u8] {
        match self {
            Self::Write(io) => &(io.data),
            Self::Flush(_) => panic!("can not deref FlushIo"),
        }
    }

    // OP_FLUSH or F_META
    #[inline]
    pub(crate) fn seq(&self) -> u64 {
        match self {
            Self::Write(io) => io.seq,
            Self::Flush(io) => io.seq,
        }
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

pub(crate) struct TgtPendingBlocksPool<T> {
    pub(crate) replica_path: String,
    pub(crate) replica_device: T,
    pub(crate) rx: channel::Receiver<Vec<PendingIo>>,
    pub(crate) tx: channel::Sender<Vec<PendingIo>>,
    // staging queue
    pub(crate) staging_data_queue: Vec<PendingIo>, // for data set without seq flag
    pub(crate) staging_data_queue_bytes: usize,
    pub(crate) staging_seq_queue: BTreeMap<u64, (Vec<PendingIo>, usize)>, // for data set with seq
    pub(crate) staging_seq_queue_bytes: usize,
    // pending queue for write to replica
    pub(crate) pending_queue: Vec<PendingIo>,
    pub(crate) pending_bytes: usize,
    pub(crate) inflight_bytes: usize, // track inflight bytes logging to replica
    pub(crate) max_capacity: usize,
    pub(crate) meta_dev_desc: MetaDeviceDesc,
    pub(crate) dev_id: u32,
    pub(crate) g_seq: IncrSeq,
    pub(crate) l_seq: u64,
}

impl<T> TgtPendingBlocksPool<T> {
    pub(crate) fn new(max_capacity: usize, replica_path: &str, replica_device: T, meta_dev_desc: MetaDeviceDesc, dev_id: u32, g_seq: IncrSeq) -> Self
        where T: Replica + 'static
    {
        let (tx, rx) = channel::unbounded();
        Self {
            rx: rx,
            tx: tx,
            staging_data_queue: Vec::new(),
            staging_data_queue_bytes: 0,
            staging_seq_queue: BTreeMap::new(),
            staging_seq_queue_bytes: 0,
            pending_queue: Vec::new(),
            pending_bytes: 0,
            inflight_bytes: 0,
            max_capacity: max_capacity,
            replica_path: replica_path.to_string(),
            replica_device: replica_device,
            meta_dev_desc,
            dev_id,
            g_seq,
            l_seq: 1,
        }
    }

    pub(crate) fn get_stats(&self) -> PoolStats {
        PoolStats {
            staging_data_queue_len: self.staging_data_queue.len(),
            staging_data_queue_bytes: self.staging_data_queue_bytes,
            staging_seq_queue_len: self.staging_seq_queue.len(),
            staging_seq_queue_bytes: self.staging_seq_queue_bytes,
            pending_queue_len: self.pending_queue.len(),
            pending_bytes: self.pending_bytes,
            inflight_bytes: self.inflight_bytes,
            max_capacity: self.max_capacity,
        }
    }

    pub(crate) fn get_tx_chan(&self) -> channel::Sender<Vec<PendingIo>> {
        self.tx.clone()
    }

    pub(crate) fn is_logging_active(&self) -> bool {
        self.inflight_bytes > 0
    }

    pub(crate) async fn main_loop(pool: Rc<RefCell<Self>>, state: Rc<GlobalTgtState>,
            region: Rc<Region>, _recover: Rc<RecoverCtrl>, task_state: TaskState)
        where T: Replica + 'static
    {
        info!("TgtPendingBlocksPool started with:");
        info!("  - state {:?}", state);
        info!("  - global seq {} local seq {}", pool.borrow().g_seq.get(), pool.borrow().l_seq);
        info!("  - region {:?}", region);
        info!("  - replica path {}", pool.borrow().replica_path);
        let rx = pool.borrow().rx.clone();
        let replica_device = pool.borrow().replica_device.dup().await;
        replica_device.open();
        task_state.set_start(TaskId::Pool);
        while let Ok(mut v) = rx.recv().await {
            // # go through pending io vec to track pending bytes/ max seq
            let mut total_bytes = 0;
            let mut max_seq = 0;
            for pio in v.iter() {
                total_bytes += pio.size();
                max_seq = std::cmp::max(max_seq, pio.seq());
            }
            pool.borrow_mut().pending_bytes += total_bytes;
            // # update staging data/seq queue with received pending io
            if max_seq == 0 {
                pool.borrow_mut().staging_data_queue.append(&mut v);
                pool.borrow_mut().staging_data_queue_bytes += total_bytes;
            } else {
                let None = pool.borrow_mut().staging_seq_queue.insert(max_seq, (v, total_bytes)) else {
                    panic!("duplicate glboal seq {} received", max_seq);
                };
                pool.borrow_mut().staging_seq_queue_bytes += total_bytes;
            }
            // # try to move data from staging queue to pending queue based on continues seq
            // check any seq we can find in stating seq queue
            let mut continue_seq_to_take = Vec::new();
            let keys: Vec<u64> = pool.borrow().staging_seq_queue.keys().cloned().collect();
            for seq in keys.into_iter() {
                if pool.borrow().l_seq != seq {
                    // if not the seq we waiting for
                    debug!("TgtPendingBlocksPool main task - seq discontinued at {}", pool.borrow().l_seq);
                    break;
                }
                continue_seq_to_take.push(seq);
                pool.borrow_mut().l_seq = seq + 1;
            }
            // handle staging data queue before staging seq queue
            if continue_seq_to_take.len() > 0 {
                // take all in staging data queue
                let mut v = pool.borrow_mut().staging_data_queue.drain(..).collect();
                pool.borrow_mut().staging_data_queue_bytes = 0;
                pool.borrow_mut().pending_queue.append(&mut v);
            }
            // finally move all continue seq from seq queue into pending queue
            // TODO: replace for loop by btreemap split_off
            for seq in continue_seq_to_take.iter() {
                // take continues seq from stating seq queue
                let (mut v, bytes) = pool.borrow_mut().staging_seq_queue.remove(seq).expect("failed to get back pending io vec from staging seq queue");
                pool.borrow_mut().staging_seq_queue_bytes -= bytes;
                pool.borrow_mut().pending_queue.append(&mut v);
            }
            // now, all seq pending io and scatter data pending io will be put on pending queue
            // # end of queue prepare process

            // test if replica is available
            if pool.borrow().is_logging_active() { continue; }

            // # time to trigger write to replica
            // # write to replica from pending queue
            if pool.borrow().pending_bytes >= pool.borrow().max_capacity {
                debug!("TgtPendingBlocksPool main task - {} of pending IO, total {} bytes exceed max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);

                state.set_logging_disable();
                // TODO: cancel all pending io after logging disabled

                // take all in staging data queue
                let mut v = pool.borrow_mut().staging_data_queue.drain(..).collect();
                pool.borrow_mut().staging_data_queue_bytes = 0;
                pool.borrow_mut().pending_queue.append(&mut v);

                let pending: Vec<PendingIo> = pool.borrow_mut().pending_queue.drain(..).collect();
                let bytes: usize = pending.iter().map(|pio| pio.size()).sum();

                pool.borrow_mut().inflight_bytes = bytes;
                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);
                pool.borrow_mut().inflight_bytes = 0;
                pool.borrow_mut().pending_bytes -= bytes;
            } else if pool.borrow().pending_bytes >= pool.borrow().max_capacity / 2 {
                debug!("TgtPendingBlocksPool main task - {} of pending IO, total {} bytes exceed 1/2 max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                // TODO: change process to
                // 1. add periodic flusher
                // 2. find last FLUSH in the queue and take out, leave remains in the queue
                // 3. write_to_replica
                let pending: Vec<PendingIo> = pool.borrow_mut().pending_queue.drain(..).collect();
                let bytes: usize = pending.iter().map(|pio| pio.size()).sum();

                pool.borrow_mut().inflight_bytes = bytes;
                let segid = replica_device.log_pending_io(pending, false).await.expect("failed to log pending io");
                assert!(segid == 0);
                pool.borrow_mut().inflight_bytes = 0;
                pool.borrow_mut().pending_bytes -= bytes;
            }
            // yield to prevent long term occupation of this task
            smol::future::yield_now().await;
        }
        info!("TgtPendingBlocksPool quit");
    }
}
