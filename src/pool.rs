use std::fmt;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, BTreeMap};
#[cfg(feature="piopr")]
use std::sync::Arc;
use libublk::sys::ublksrv_io_desc;
use libublk::helpers::IoBuf;
use smol::channel;
use log::{error, info, debug, trace};
use crate::state::GlobalTgtState;
use crate::region::Region;
use crate::recover::RecoverCtrl;
use crate::replica::Replica;
use crate::device::MetaDevice;
use crate::task::{TaskState, TaskId};
use crate::seq::IncrSeq;
use crate::stats::PoolStats;

#[cfg(feature="piopr")]
pub(crate) struct PendingIoMeta {
    pub(crate) flags: u32,
    pub(crate) size: u32,
    pub(crate) offset: u64,
    pub(crate) seq: u64,
}

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

    #[cfg(feature="piopr")]
    #[inline]
    pub(crate) fn to_meta(&self) -> Option<PendingIoMeta> {
        match self {
            Self::Write(io) => {
                return Some(PendingIoMeta {
                    flags: io.flags,
                    size: io.size,
                    offset: io.offset,
                    seq: io.seq,
                });
            },
            Self::Flush(io) => {
                return None;
            },
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
    pub(crate) meta_dev: MetaDevice,
    pub(crate) dev_id: u32,
    pub(crate) g_seq: IncrSeq,
    pub(crate) l_seq: u64,
    #[cfg(feature="piopr")]
    pub(crate) piopr: Arc<crate::region::PendingIoPersistRegionMap>,
    #[cfg(feature="piopr")]
    // tracking pending io wait for flush
    pub(crate) flush_queue: Vec<PendingIoMeta>,
}

impl<T> TgtPendingBlocksPool<T> {
    #[cfg(not(feature="piopr"))]
    pub(crate) fn new(max_capacity: usize, replica_path: &str, replica_device: T, meta_dev: MetaDevice, dev_id: u32, g_seq: IncrSeq) -> Self
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
            meta_dev,
            dev_id,
            g_seq,
            l_seq: 1,
        }
    }

    #[cfg(feature="piopr")]
    pub(crate) fn new_with_piopr(max_capacity: usize, replica_path: &str, replica_device: T, meta_dev: MetaDevice, dev_id: u32, g_seq: IncrSeq,
            piopr: Arc<crate::region::PendingIoPersistRegionMap>) -> Self
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
            meta_dev,
            dev_id,
            g_seq,
            l_seq: 1,
            piopr: piopr,
            flush_queue: Vec::new(),
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

    pub(crate) fn is_replica_busy(&self) -> bool {
        self.inflight_bytes > 0
    }

    // to handler all cases that pending io should log as dirty region
    // as result
    // all pending ios in staging_data_queue/staging_seq_queue/pending_queue marked as dirty reion
    // return:
    //   - list of all dirty region should be log
    pub(crate) fn log_as_dirty_region_in_memory(pool: Rc<RefCell<Self>>, region: Rc<Region>, state: Rc<GlobalTgtState>, inflight_dirty_regions: Vec<u64>) -> Vec<u64> {
        assert!(!state.is_logging_enabled());
        let mut ids = inflight_dirty_regions;
        for io in pool.borrow().staging_data_queue.iter() {
            let id = region.region_id(io.offset(), io.size() as u64);
            ids.push(id);
        }
        for (v, _) in pool.borrow().staging_seq_queue.values() {
            for io in v.iter() {
                let id = region.region_id(io.offset(), io.size() as u64);
                ids.push(id);
            }
        }
        for io in pool.borrow().pending_queue.iter() {
            let id = region.region_id(io.offset(), io.size() as u64);
            ids.push(id);
        }
        ids.dedup();

        // mark all pending io covered region dirty
        for region_id in ids.iter() {
            region.mark_dirty_region_id(*region_id);
        }
        return ids;
    }

    fn pool_clear_all_pending(pool: Rc<RefCell<Self>>) {
        // cleanup queue in pool
        pool.borrow_mut().inflight_bytes = 0;
        pool.borrow_mut().staging_data_queue.clear();
        pool.borrow_mut().staging_data_queue_bytes = 0;
        pool.borrow_mut().staging_seq_queue.clear();
        pool.borrow_mut().staging_seq_queue_bytes = 0;
        pool.borrow_mut().pending_queue.clear();
        pool.borrow_mut().pending_bytes = 0;
    }

    // return
    //   - true: logged something or nothing
    //   - false: logging to replica failed, global logging disabled
    pub(crate) async fn try_log_pending(pool: Rc<RefCell<Self>>, replica_device: &T, region: Rc<Region>, state: Rc<GlobalTgtState>) -> bool
        where T: Replica + 'static
    {
        if pool.borrow().pending_bytes == 0 {
            return true;
        }
        // take all in staging data queue
        let mut v = pool.borrow_mut().staging_data_queue.drain(..).collect();
        pool.borrow_mut().staging_data_queue_bytes = 0;
        pool.borrow_mut().pending_queue.append(&mut v);
        // tak all from pending queue
        let pending: Vec<PendingIo> = pool.borrow_mut().pending_queue.drain(..).collect();
        let bytes: usize = pending.iter().map(|pio| pio.size()).sum();
        debug!("TgtPendingBlocksPool try log pending - {}/{} bytes/total pending", bytes, pool.borrow().pending_bytes);

        assert!(pool.borrow().inflight_bytes == 0);
        // save dirty regions
        let mut inflight_dirty_regions: Vec<u64> = pending.iter().map(|io| region.region_id(io.offset(), io.size() as u64)).collect();
        inflight_dirty_regions.dedup();
        pool.borrow_mut().inflight_bytes = bytes;
        let start = std::time::Instant::now();
        #[cfg(feature="piopr")]
        let mut v_iometa: Vec<PendingIoMeta> = pending.iter().filter_map(|io| io.to_meta()).collect();
        #[cfg(feature="piopr")]
        debug!("TgtPendingBlocksPool try log pending - convert pending io to io meta cost: {:?}", start.elapsed());
        let Ok(segid) = replica_device.log_pending_io(pending, false).await else {
            state.set_logging_disable();
            let dirty_regions = Self::log_as_dirty_region_in_memory(pool.clone(), region, state, inflight_dirty_regions);
            let res = pool.borrow().meta_dev.preg_mark_dirty_batch(dirty_regions).await;
            if res.is_err() {
                error!("TgtPendingBlocksPool - failed to sync persist region map {:?}", res);
                // TODO: handle meta device failed case
            }
            Self::pool_clear_all_pending(pool.clone());
            return false;
        };
        debug!("TgtPendingBlocksPool try log pending - log pending io to replica device cost: {:?}", start.elapsed());
        assert!(segid == 0);
        #[cfg(feature="piopr")]
        pool.borrow_mut().flush_queue.append(&mut v_iometa);
        pool.borrow_mut().inflight_bytes = 0;
        pool.borrow_mut().pending_bytes -= bytes;
        debug!("TgtPendingBlocksPool try log pending - {} bytes appended to replica cost {:?}", bytes, start.elapsed());
        true
    }

    pub(crate) fn process_pending(&mut self, input: Vec<PendingIo>) {
        // # go through pending io vec to track pending bytes/ max seq
        // group input vec of pending io by seq if we can
        let mut total_bytes = 0;
        let mut max_seq = 0;
        let mut seq_grp_bytes = 0;
        let mut seq_map: HashMap<u64, (Vec<PendingIo>, usize)> = HashMap::new();
        let mut v = Vec::new(); // temp list to hold Non-seq pending io
        for pio in input.into_iter() {
            total_bytes += pio.size();
            seq_grp_bytes += pio.size();
            let pio_seq = pio.seq();
            v.push(pio);
            if pio_seq > 0 {
                let mut grp = Vec::new();
                grp.append(&mut v);
                // create and insert new group to seq map
                let None = seq_map.insert(pio_seq, (grp, seq_grp_bytes)) else {
                    panic!("duplicate seq {} from input", pio_seq);
                };
                seq_grp_bytes = 0;
                max_seq = std::cmp::max(max_seq, pio_seq);
            }
        }
        self.pending_bytes += total_bytes;
        // # update staging data/seq queue with received pending io
        if max_seq == 0 {
            assert!(seq_map.len() == 0);
            self.staging_data_queue.append(&mut v);
            self.staging_data_queue_bytes += total_bytes;
        } else {
            let mut grp_bytes = 0;
            for (seq, (grp, bytes)) in seq_map.into_iter() {
                grp_bytes += bytes;
                let None = self.staging_seq_queue.insert(seq, (grp, bytes)) else {
                    panic!("duplicate glboal seq {} received", seq);
                };
            }
            self.staging_seq_queue_bytes += grp_bytes;
        }
        // # try to move data from staging queue to pending queue based on continues seq
        // check any seq we can find in stating seq queue
        let mut continue_seq_to_take = Vec::new();
        let keys: Vec<u64> = self.staging_seq_queue.keys().cloned().collect();
        for seq in keys.into_iter() {
            if self.l_seq != seq {
                // if not the seq we waiting for
                trace!("TgtPendingBlocksPool main task - seq discontinued at {} for {}", self.l_seq, seq);
                break;
            }
            continue_seq_to_take.push(seq);
            self.l_seq = seq + 1;
            trace!("TgtPendingBlocksPool main task - seq continue to {} from {}", self.l_seq, seq);
        }
        // handle staging data queue before staging seq queue
        if continue_seq_to_take.len() > 0 {
            // take all in staging data queue
            let mut v = self.staging_data_queue.drain(..).collect();
            self.staging_data_queue_bytes = 0;
            self.pending_queue.append(&mut v);
        }
        // finally move all continue seq from seq queue into pending queue
        // TODO: replace for loop by btreemap split_off
        for seq in continue_seq_to_take.iter() {
            // take continues seq from stating seq queue
            let (mut v, bytes) = self.staging_seq_queue.remove(seq).expect("failed to get back pending io vec from staging seq queue");
            self.staging_seq_queue_bytes -= bytes;
            self.pending_queue.append(&mut v);
        }
        // now, all seq pending io and scatter data pending io has been put on pending queue

        // last things:
        // if we still have some ungroup bytes, let's put this onto staging_data_queue,
        // let them waiit on future seq
        if v.len() > 0 {
            let bytes: usize = v.iter().map(|pio| pio.size()).sum();
            self.staging_data_queue.append(&mut v);
            self.staging_data_queue_bytes += bytes;
        }
        // # end of queue prepare process
    }

    pub(crate) async fn idle() -> std::result::Result<Vec<PendingIo>, smol::channel::RecvError> {
        smol::Timer::after(std::time::Duration::from_secs(1)).await;
        Ok(Vec::new())
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
        while let Ok(v) = smol::future::or(rx.recv(), Self::idle()).await
        {
            let is_timeout = v.len() == 0;

            if !is_timeout {
                // pending io received from channel
                pool.borrow_mut().process_pending(v);
            }

            // logging disabled, log dirty region instead
            if !state.is_logging_enabled() {
                let dirty_regions = Self::log_as_dirty_region_in_memory(pool.clone(), region.clone(), state.clone(), Vec::new());
                let res = pool.borrow().meta_dev.preg_mark_dirty_batch(dirty_regions).await;
                if res.is_err() {
                    error!("TgtPendingBlocksPool - failed to sync persist region map {:?}", res);
                    // TODO: handle meta device failed case
                }
                Self::pool_clear_all_pending(pool.clone());
                continue;
            }

            // pending bytes reached max capacity
            if pool.borrow().pending_bytes >= pool.borrow().max_capacity {
                debug!("TgtPendingBlocksPool main task - {} of pending IO, total {} bytes exceed max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);

                state.set_logging_disable();
                let dirty_regions = Self::log_as_dirty_region_in_memory(pool.clone(), region.clone(), state.clone(), Vec::new());
                let res = pool.borrow().meta_dev.preg_mark_dirty_batch(dirty_regions).await;
                if res.is_err() {
                    error!("TgtPendingBlocksPool - failed to sync persist region map {:?}", res);
                    // TODO: handle meta device failed case
                }
                Self::pool_clear_all_pending(pool.clone());
                continue;
            }

            // if rx is not empty, chance to get more
            if !rx.is_empty() {
                continue;
            }

            // test if replica is busy
            if pool.borrow().is_replica_busy() {
                debug!("TgtPendingBlocksPool main task - replica is busy");
                // wait another tick when replica is busy
                continue;
            }

            // # time to trigger write to replica

            if is_timeout {
                // idle timeout
                // kick log pending
                let _ = Self::try_log_pending(pool.clone(), &replica_device, region.clone(), state.clone()).await;
                continue;
            }

            // # write to replica from pending queue
            if pool.borrow().pending_bytes >= pool.borrow().max_capacity / 2 {
                debug!("TgtPendingBlocksPool main task - {} of pending IO, total {} bytes exceed 1/2 max capacity {}",
                    pool.borrow().pending_queue.len(), pool.borrow().pending_bytes, pool.borrow().max_capacity);
                let stats = pool.borrow().get_stats();
                if stats.is_active() {
                    debug!("TgtPendingBlocksPool main task - {}", stats);
                }

                // kick log pending
                let _ = Self::try_log_pending(pool.clone(), &replica_device, region.clone(), state.clone()).await;
            }
        }
        info!("TgtPendingBlocksPool quit");
    }
}
