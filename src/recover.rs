use std::fmt;
use std::io::Result;
use std::rc::Rc;
use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::collections::{HashMap, VecDeque, HashSet, BTreeMap};
use smol::channel;
use smol::lock::{Mutex, RwLock};
use smol::Executor;
use smol::lock::Semaphore;
use smol::fs::{OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncSeekExt, AsyncReadExt, AsyncWriteExt};
use libublk::sys::ublksrv_io_desc;
use serde_repr::*;
use log::debug;
use crate::state::GlobalTgtState;
use crate::state;
use crate::region;
use crate::io_replica::LOCAL_RECOVER_CTRL;
use crate::replica::Replica;
use crate::task::{TaskState, TaskId};
use crate::pool::{TgtPendingBlocksPool, PendingIo};

const DEFAULT_FORWARD_FINAL_TRANSITION_THRESHOLD: usize = 4;

const RECOVERY_WAIT_ON_MS: u64 = 50;
const RECOVERY_FINAL_WAIT_INTERVAL_MS: u64 = 10;

#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub(crate) enum RecoverState {
    NoSync      = 0, // not yet sync
    Recovering  = 1, // doing recovery
    Clean       = 2, // in synced
    Dirty       = 3, // new write after synced
}

#[allow(dead_code)]
const MIN_ALIGNED: usize = 4096;

#[allow(dead_code)]
pub(crate) struct Region {
    pub(crate) state: RecoverState,
    pub(crate) id: u64,
    ptr: *mut u8, // buffer ptr for region in memory data mirror
    size: usize,  // size of region
}

#[allow(dead_code)]
unsafe impl Send for Region {}

#[allow(dead_code)]
impl Region {
    fn new(id: u64, state: RecoverState) -> Self {
        Self { state: state, id: id, ptr: std::ptr::null_mut(), size: 0 }
    }

    fn alloc(&mut self, size: usize) {
        let layout = std::alloc::Layout::from_size_align(size, MIN_ALIGNED).expect("unable to create layout for aligned block");
        self.ptr = unsafe { std::alloc::alloc(layout) };
        self.size = size;
    }

    // load region from primary device
    async fn load_from_primary(&mut self, primary: &str, region_size: u64, region_id: u64) -> Result<()> {
        assert!(!self.ptr.is_null());
        assert!(self.size == region_size as usize);
        let mut primary_dev = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(primary)
                .await.expect("unable to open primary device");

        let offset = region_size * region_id;
        let buf = unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.size)
        };
        primary_dev.seek(smol::io::SeekFrom::Start(offset)).await?;
        primary_dev.read_exact(buf).await?;
        Ok(())
    }

    // write pending io into region buffer
    fn write(&self, io: PendingIo, region_size: u64, region_id: u64) {
        let p_offset = io.offset();
        let p_size = io.data_size();
        // check region id is correct
        assert!(region_id == p_offset / region_size);
        // check data size is not exceed region boundary
        assert!(region_id == (p_offset + p_size as u64) / region_size);
        let offset = (p_offset % region_size) as usize;
        let buf: &mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(self.ptr, self.size)
        };
        let chunk = &mut buf[offset..offset+p_size];
        chunk.copy_from_slice(io.as_ref());
    }
}

#[derive(Clone)]
pub(crate) struct RecoverCtrl {
    primary_path: String,
    replica_path: String,
    region_map: Arc<RwLock<HashMap<u64, Arc<Mutex<Region>>>>>,
    queue: Arc<Mutex<VecDeque<(u64, Arc<Mutex<Region>>)>>>,
    mode: Arc<RwLock<u64>>,
    g_state: GlobalTgtState,
    region_size: u64,
    nr_regions: u64,
    inflight: Arc<AtomicU64>,
    pending: Arc<AtomicU64>,
    concurrency: Arc<AtomicUsize>,
    tx: channel::Sender<bool>,
    rx: channel::Receiver<bool>,
    // track regions dirty again during recover process
    region_dirty_again: Arc<RwLock<HashSet<u64>>>,
}

impl fmt::Debug for RecoverCtrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode_lock = self.mode.try_read_arc().expect("unable to get read lock for mode");
        let lock = self.region_map.try_read_arc().expect("unable to get read lock for region map");
        let total = (*lock).len();

        let mut inflight = 0;
        let mut pending = 0;
        for (id, region) in self.queue.try_lock_arc().expect("unable to get read lock for queue").iter() {
            let region_lock = region.try_lock_arc().expect("unable to get read lock for recover region");
            match (*region_lock).state {
                RecoverState::Recovering => { inflight += 1; },
                RecoverState::NoSync => { pending += 1; },
                s @ _ => { panic!("invalide region {} state {:?} in recovery queue", id, s); },
            }
        }

        let mode = *mode_lock;
        drop(mode_lock);
        write!(f, "RecoverCtrl {{ primary: {}, replica: {}, mode: {}, queue: inflight/pending/total {}/{}/{} }}",
            self.primary_path, self.replica_path, mode, inflight, pending, total)
    }
}

impl Default for RecoverCtrl {
    fn default() -> Self {
        let (tx, rx) = channel::bounded(1);
        Self {
            primary_path: String::new(),
            replica_path: String::new(),
            region_map: Arc::new(RwLock::new(HashMap::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            mode: Arc::new(RwLock::new(state::TGT_STATE_LOGGING_ENABLED)),
            g_state: GlobalTgtState::new(),
            region_size: 0,
            nr_regions: 0,
            inflight: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(AtomicU64::new(0)),
            concurrency: Arc::new(AtomicUsize::new(0)),
            tx: tx,
            rx: rx,
            region_dirty_again: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

impl RecoverCtrl {
    pub(crate) fn with_region_size(mut self, region_size: u64) -> Self {
        self.region_size = region_size;
        self
    }

    pub(crate) fn with_nr_regions(mut self, nr_regions: u64) -> Self {
        self.nr_regions = nr_regions;
        self
    }

    pub(crate) fn with_g_state(mut self, g_state: GlobalTgtState) -> Self {
        self.g_state = g_state;
        self
    }

    pub(crate) fn with_primary_path(mut self, path: &str) -> Self {
        self.primary_path = path.to_string();
        self
    }

    pub(crate) fn with_replica_path(mut self, path: &str) -> Self {
        self.replica_path = path.to_string();
        self
    }

    pub(crate) fn with_concurrency(self, n: usize) -> Self {
        self.concurrency.store(n, Ordering::SeqCst);
        self
    }

    #[inline]
    fn inc_inflight(&self, i: u64) {
        self.inflight.fetch_add(i, Ordering::SeqCst);
    }

    #[inline]
    fn dec_inflight(&self, i: u64) {
        self.inflight.fetch_sub(i, Ordering::SeqCst);
    }

    #[inline]
    fn get_inflight(&self) -> u64 {
        self.inflight.load(Ordering::SeqCst)
    }

    #[inline]
    fn inc_pending(&self, i: u64) {
        self.pending.fetch_add(i, Ordering::SeqCst);
    }

    #[inline]
    fn dec_pending(&self, i: u64) {
        self.pending.fetch_sub(i, Ordering::SeqCst);
    }

    #[inline]
    fn get_pending(&self) -> u64 {
        self.pending.load(Ordering::SeqCst)
    }

    // init ctrl with all region in state NoSync
    #[allow(dead_code)]
    pub(crate) fn new(region_size: u64, nr_regions: u64, mode: u64, primary: &str, replica: &str) -> Self {
        let mut map = HashMap::new();
        for i in 0..nr_regions {
            map.insert(i, Arc::new(Mutex::new(Region::new(i, RecoverState::NoSync))));
        }

        // sort by key
        let mut sorted: Vec<_> = map.iter().collect();
        sorted.sort_by_key(|i| i.0);

        let mut queue = VecDeque::new();
        for (region_id, region) in sorted.into_iter() {
            queue.push_back((*region_id, region.clone()));
        }
        let (tx, rx) = channel::bounded(1);
        Self {
            primary_path: primary.to_string(),
            replica_path: replica.to_string(),
            region_map: Arc::new(RwLock::new(map)),
            queue: Arc::new(Mutex::new(queue)),
            mode: Arc::new(RwLock::new(mode)),
            g_state: GlobalTgtState::new(),
            region_size: region_size,
            nr_regions: nr_regions,
            inflight: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(AtomicU64::new(nr_regions)),
            concurrency: Arc::new(AtomicUsize::new(0)),
            tx: tx,
            rx: rx,
            region_dirty_again: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    fn init_no_sync(nr_regions: u64) -> (HashMap<u64, Arc<Mutex<Region>>>, VecDeque<(u64, Arc<Mutex<Region>>)>) {
        let mut map = HashMap::new();
        for i in 0..nr_regions {
            map.insert(i, Arc::new(Mutex::new(Region::new(i, RecoverState::NoSync))));
        }

        // sort by key
        let mut sorted: Vec<_> = map.iter().collect();
        sorted.sort_by_key(|i| i.0);

        let mut queue = VecDeque::new();
        for (region_id, region) in sorted.into_iter() {
            queue.push_back((*region_id, region.clone()));
        }
        (map, queue)
    }

    fn rebuild_mode_full(&self, mode: u64) {
        // mode as whole transaction lock
        let mut mode_lock = self.mode.try_write_arc().expect("unable to get write lock for mode");

        // we can now safely change global state
        self.g_state.clear_all_recover_bits();
        match mode {
            state::TGT_STATE_RECOVERY_FORWARD_FULL => {
                self.g_state.set_recovery_forward_full();
            },
            state::TGT_STATE_RECOVERY_REVERSE_FULL => {
                self.g_state.set_recovery_reverse_full();
            },
            _ => {
                panic!("rebuld_mode_full - invalid mode {}", mode);
            }
        }

        let (region_map, queue) = Self::init_no_sync(self.nr_regions);

        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = region_map;

        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        self.inc_pending(self.nr_regions);

        // release mode lock
        *mode_lock = mode;
    }

    pub(crate) fn rebuild_mode_reverse_full(&self) {
        self.rebuild_mode_full(state::TGT_STATE_RECOVERY_REVERSE_FULL);
    }

    pub(crate) fn rebuild_mode_forward_full(&self) {
        self.rebuild_mode_full(state::TGT_STATE_RECOVERY_FORWARD_FULL);
    }

    pub(crate) fn rebuild_mode_forward_part(&self, g_region: Rc<region::Region>, v_region_dirty_again: Vec<u64>) {
        // mode as whole transaction lock
        let mut mode_lock = self.mode.try_write_arc().expect("unable to get write lock for mode");

        // get all dirty regions
        let v = g_region.collect();
        // rebuild
        let mut map = HashMap::new();
        let nr_regions = g_region.nr_regions();
        for i in 0..nr_regions {
            map.insert(i, Arc::new(Mutex::new(Region::new(i, RecoverState::Clean))));
        }
        // update NoSync region
        let mut nosync = Vec::new();
        for i in v.iter() {
            let region = Arc::new(Mutex::new(Region::new(*i, RecoverState::NoSync)));
            map.insert(*i, region.clone());
            nosync.push((*i, region));
        }
        // try merge with region dirty again
        for i in v_region_dirty_again.iter() {
            let region = Arc::new(Mutex::new(Region::new(*i, RecoverState::NoSync)));
            map.insert(*i, region.clone());
            nosync.push((*i, region));
        }
        if v_region_dirty_again.len() > 0 {
            nosync.dedup_by_key(|i| i.0);
        }

        // sort by key
        let mut sorted: Vec<_> = nosync.iter().collect();
        sorted.sort_by_key(|i| i.0);

        // build queue for NoSync region
        let mut queue = VecDeque::new();
        for (region_id, region) in sorted.into_iter() {
            queue.push_back((*region_id, region.clone()));
        }

        self.inc_pending(queue.len() as u64);

        // update inner
        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = map;

        // reset global region and clear dirty flag
        g_region.reset();

        // we can now safely change global state
        if self.g_state.is_logging_enabled() {
            self.g_state.set_recovery_forward_part();
        } else {
            self.g_state.set_recovery_forward_part_logging_disabled();
        }

        // release mode lock
        *mode_lock = state::TGT_STATE_RECOVERY_FORWARD_PART;
    }

    pub(crate) fn rebuild_mode_forward_final(&self, g_region: Rc<region::Region>, v_region_dirty_again: Vec<u64>) {
        // mode as whole transaction lock
        let mut mode_lock = self.mode.try_write_arc().expect("unable to get write lock for mode");

        // get all dirty regions
        let v = g_region.collect();
        // rebuild
        let mut map = HashMap::new();
        let nr_regions = g_region.nr_regions();
        for i in 0..nr_regions {
            map.insert(i, Arc::new(Mutex::new(Region::new(i, RecoverState::Clean))));
        }
        // update NoSync region
        let mut nosync = Vec::new();
        for i in v.iter() {
            let region = Arc::new(Mutex::new(Region::new(*i, RecoverState::NoSync)));
            map.insert(*i, region.clone());
            nosync.push((*i, region));
        }
        // try merge with region dirty again
        for i in v_region_dirty_again.iter() {
            let region = Arc::new(Mutex::new(Region::new(*i, RecoverState::NoSync)));
            map.insert(*i, region.clone());
            nosync.push((*i, region));
        }
        if v_region_dirty_again.len() > 0 {
            nosync.dedup_by_key(|i| i.0);
        }

        // sort by key
        let mut sorted: Vec<_> = nosync.iter().collect();
        sorted.sort_by_key(|i| i.0);

        // build queue for NoSync region
        let mut queue = VecDeque::new();
        for (region_id, region) in sorted.into_iter() {
            queue.push_back((*region_id, region.clone()));
        }

        self.inc_pending(queue.len() as u64);

        // update inner
        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = map;

        // reset global region and clear dirty flag
        g_region.reset();

        // we can now safely change global state
        if self.g_state.is_logging_enabled() {
            self.g_state.set_recovery_forward_final();
        } else {
            self.g_state.set_recovery_forward_final_logging_disabled();
        }

        // release mode lock
        *mode_lock = state::TGT_STATE_RECOVERY_FORWARD_FINAL;
    }

    pub(crate) fn mode(&self) -> u64 {
        *self.mode.read_arc_blocking()
    }

    #[inline]
    async fn lookup(&self, region_id: u64) -> Arc<Mutex<Region>> {
        if let Some(region) = self.region_map.read_arc().await.get(&region_id) {
            return region.clone();
        }
        panic!("unable to find region {} in region map", region_id);
    }

    // re-priority of specific region by it's id
    pub(crate) async fn put_high_prio(&self, region_id: u64) {
        let mut queue = self.queue.lock().await;
        // find region in prio queue
        let mut found = None;
        for (idx, r) in queue.iter().enumerate() {
            if r.0 == region_id {
                found = Some(idx);
                break;
            }
        }
        // if found, put to front
        if let Some(idx) = found {
            let (id, region) = queue.remove(idx).expect("unable to take out region from prio queue by it's index");
            queue.push_front((id, region));
        }
    }

    // fetch one candidate from top of prio queue
    pub(crate) async fn fetch_one(&self) -> Option<(u64, Arc<Mutex<Region>>)> {
        self.queue.lock().await.pop_front()
    }

    // infinite loop wait on region state from NoSync || Recovering -> Dirty || Clean
    // return:
    //   - true: leave recovery process
    //   - false: still in recovery process
    pub(crate) async fn wait_on(&self, region: Arc<Mutex<Region>>) -> bool {
        loop {
            let lock = region.lock().await;
            if lock.state == RecoverState::Dirty || lock.state == RecoverState::Clean {
                if !self.g_state.is_recovery() {
                    return true;
                }
                return false;
            }
            drop(lock);
            smol::Timer::after(std::time::Duration::from_millis(RECOVERY_WAIT_ON_MS)).await;
            // if after wait timeout, we are quit recovery process, let's quit
            if !self.g_state.is_recovery() {
                return true;
            }
        }
    }

    pub(crate) async fn wait_on_forward_final(&self) {
        while !self.g_state.is_not_recovery_and_logging_enabled() {
            smol::Timer::after(std::time::Duration::from_millis(RECOVERY_WAIT_ON_MS)).await;
        }
    }

    // read op run in queue executor context
    pub(crate) async fn q_recover_read(&self, region_id: u64) {
        let mode = *self.mode.read_arc().await;
        if mode == state::TGT_STATE_RECOVERY_FORWARD_FULL
            || mode == state::TGT_STATE_RECOVERY_FORWARD_PART
            || mode == state::TGT_STATE_RECOVERY_FORWARD_FINAL
        {
            // in forward mode, data on primary is updated,
            // no need to wait for read op
            return;
        }

        // in reverse recovery mode
        let r = self.lookup(region_id).await;
        let region = r.lock().await;
        let state = region.state;
        match state {
            RecoverState::NoSync => {
                // trigger recover of this region in high priority
                self.put_high_prio(region_id).await;
                drop(region);
                self.wait_on(r.clone()).await;
            },
            RecoverState::Recovering => {
                drop(region);
                self.wait_on(r.clone()).await;
            },
            _ => {
            },
        };
        // handle RecoverState::Clean and RecoverState::Dirty
        // region reverse recovery is done, let do read
        return;
    }

    // write op run in queue executor context
    pub(crate) async fn q_recover_write(&self, region_id: u64) {
        if self.g_state.is_recovery_forward_final() {
            self.wait_on_forward_final().await;
            return;
        }

        // in both forward and reverse mode, write io will be blocked
        let r = self.lookup(region_id).await;
        let region = r.lock().await;
        let state = region.state;
        match state {
            RecoverState::NoSync => {
                // trigger recover of this region in high priority
                self.put_high_prio(region_id).await;
                drop(region);
                if self.wait_on(r.clone()).await {
                    return;
                }
                // mark this region dirty in dirty again list
                let _ = self.region_dirty_again.write().await.insert(region_id);
            },
            RecoverState::Recovering => {
                drop(region);
                if self.wait_on(r.clone()).await {
                    return;
                }
                // mark this region dirty in dirty again list
                let _ = self.region_dirty_again.write().await.insert(region_id);
            },
            RecoverState::Clean => {
                // mark this region dirty in dirty again list
                let _ = self.region_dirty_again.write().await.insert(region_id);
            },
            RecoverState::Dirty => {
            },
        };
        return;
    }

    // copy one region
    async fn recover_worker<T: Replica>(inflight: Arc<AtomicU64>, pending: Arc<AtomicU64>,
            primary: String, replica: T, forward: bool,
            region_id: u64, region_size: u64, region: Arc<Mutex<Region>>) -> Result<()>
    {
        let mut lock = region.lock().await;
        (*lock).state = RecoverState::Recovering;
        pending.fetch_sub(1, Ordering::SeqCst);
        inflight.fetch_add(1, Ordering::SeqCst);
        drop(lock);

        let mut primary_dev = if forward {
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(&primary)
                .await.expect("unable to open primary device")
        } else {
            OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&primary)
                .await.expect("unable to open primary device")
        };

        let offset = region_size * region_id;
        let mut buf = Vec::with_capacity(region_size as usize);
        buf.resize(region_size as usize, 0);

        if forward {
            primary_dev.seek(smol::io::SeekFrom::Start(offset)).await?;
            primary_dev.read_exact(&mut buf).await?;
            replica.write(offset, &buf).await?;
        } else {
            replica.read(offset, &mut buf).await?;
            primary_dev.seek(smol::io::SeekFrom::Start(offset)).await?;
            primary_dev.write_all(&buf).await?;
            primary_dev.flush().await?;
        }

        let mut lock = region.lock().await;
        (*lock).state = RecoverState::Clean;
        inflight.fetch_sub(1, Ordering::SeqCst);
        drop(lock);

        Ok(())
    }

    // transition to:
    //  - logging enabled (recover done)
    //  - forward part again
    //  - forward final
    async fn do_state_transition<T: Replica>(&self, pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, g_region: Rc<region::Region>, task_state: TaskState) {
        // hold region_dirty_again write lock as global transition lock
        let mut lock = self.region_dirty_again.write().await;
        // time to check if total dirty region count in region dirty again and g_region during the whole recover process
        let mut all = Vec::new();
        let mut v_dirty_region_again: Vec<u64> = lock.iter().map(|i| *i).collect();
        let mut v_dirty_region: Vec<u64> = g_region.collect();
        all.append(&mut v_dirty_region_again);
        all.append(&mut v_dirty_region);
        all.dedup();
        let dirty_count = all.len();
        if dirty_count > DEFAULT_FORWARD_FINAL_TRANSITION_THRESHOLD {
            debug!("RecoverCtrl - state transition to FORWARD PART - dirty region count {} > {}", dirty_count, DEFAULT_FORWARD_FINAL_TRANSITION_THRESHOLD);
            // too much dirty regions start over forward part again
            let v_region_dirty_again: Vec<u64> = lock.drain().into_iter().collect();
            self.rebuild_mode_forward_part(g_region.clone(), v_region_dirty_again);
            self.kickoff();
            return;
        } else if dirty_count == 0 && self.g_state.is_recovery_forward_final() {
            // forward final to forward done transition
            debug!("RecoverCtrl - state transition to DONE");
            // all regions recovered, clear recover state bits
            let mut mode_lock = self.mode.write_arc().await;
            #[cfg(feature="piopr")]
            let _ = pool.borrow_mut().piopr.clear().await;
            let state = self.g_state.clear_all_recover_bits();
            // enable logging
            self.g_state.set_logging_enable();
            *mode_lock = state;
            pool.borrow().meta_dev.borrow_mut().sb_state_sync(state).await;
            task_state.set_start(TaskId::PeriodicReplicaFlush);
            return;
        }
        debug!("RecoverCtrl - state transition to FORWARD FINAL - dirty region count {}", dirty_count);
        // transition to forward_final staging
        let v_region_dirty_again: Vec<u64> = lock.drain().into_iter().collect();
        self.rebuild_mode_forward_final(g_region.clone(), v_region_dirty_again);
        self.kickoff();
        return;
    }

    // main control of recover process
    pub(crate) async fn do_recovery<'a, T: Replica + 'a>(&self, pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, g_region: Rc<region::Region>, task_state: TaskState, exec: Executor<'a>) {
        let replica = pool.borrow().replica_device.dup().await;
        // prepare recover mode
        let mode = self.mode.read_arc().await;
        let forward = if *mode == state::TGT_STATE_RECOVERY_REVERSE_FULL {
            false
        } else if *mode == state::TGT_STATE_RECOVERY_FORWARD_FULL || *mode == state::TGT_STATE_RECOVERY_FORWARD_PART || *mode == state::TGT_STATE_RECOVERY_FORWARD_FINAL {
            true
        } else {
            panic!("unkown RecoverCtrl mode {} found during kickoff recover", *mode);
        };
        pool.borrow().meta_dev.borrow_mut().sb_state_sync(*mode).await;
        drop(mode);
        if forward {
            task_state.set_stop(TaskId::PeriodicReplicaFlush);
            task_state.wait_on_stopped(TaskId::PeriodicReplicaFlush).await;
            // do a final flush
            let _ = replica.flush().await;
        }

        let sema = Arc::new(Semaphore::new(self.concurrency.load(Ordering::SeqCst)));
        let region_size = self.region_size;
        loop {
            let Some(guard) = sema.try_acquire_arc() else {
                exec.tick().await;
                continue;
            };
            // break recover process if received cmd is false or channel closed
            match self.rx.try_recv() {
                Ok(cmd) => { if !cmd { return; } },
                Err(channel::TryRecvError::Closed) => { return; },
                _ => {},
            }

            if let Some((region_id, region)) = self.fetch_one().await {
                let c_primary = self.primary_path.to_owned();
                let c_replica = replica.dup().await;
                let c_inflight = self.inflight.clone();
                let c_pending = self.pending.clone();
                let task = exec.spawn(async move {
                    let _ = Self::recover_worker(c_inflight, c_pending,
                        c_primary, c_replica, forward,
                        region_id, region_size, region).await;
                    drop(guard);
                });
                // run task in background
                task.detach();
            } else {
                break;
            }
        }

        while !exec.is_empty() {
            exec.tick().await;
        }

        // nothing left in recover queue now, let's wait for all region finished
        while self.get_inflight() > 0 || self.get_pending() > 0 {
            smol::Timer::after(std::time::Duration::from_millis(RECOVERY_FINAL_WAIT_INTERVAL_MS)).await;
            continue;
        }
        assert!(self.get_inflight() == 0 && self.get_pending() == 0);
        if !forward {
            debug!("RecoverCtrl - state transition from REVERSE FULL");
        }
        self.do_state_transition(pool, g_region, task_state.clone()).await;
    }

    #[allow(dead_code)]
    pub(crate) async fn _do_recovery(&self, _exec: Rc<smol::LocalExecutor<'_>>) {
        while let Some((region_id, region)) = self.fetch_one().await {
            let mut lock = region.lock().await;
            (*lock).state = RecoverState::Recovering;
            self.dec_pending(1);
            self.inc_inflight(1);
            drop(lock);

            let mode = self.mode.read_arc().await;
            let (from, to) = if *mode == state::TGT_STATE_RECOVERY_REVERSE_FULL {
                (self.replica_path.clone(), self.primary_path.clone())
            } else if *mode == state::TGT_STATE_RECOVERY_FORWARD_FULL || *mode == state::TGT_STATE_RECOVERY_FORWARD_PART {
                (self.primary_path.clone(), self.replica_path.clone())
            } else {
                panic!("unkown RecoverCtrl mode {} found during kickoff recover", *mode);
            };
            drop(mode);

            let mut from_dev = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(from.clone())
                .await.expect("unable to open primary device");
            let mut to_dev = OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(to.clone())
                .await.expect("unable to open replica device");

            let offset = self.region_size * region_id;
            let mut buf = Vec::with_capacity(self.region_size as usize);
            buf.resize(self.region_size as usize, 0);

            from_dev.seek(smol::io::SeekFrom::Start(offset)).await.unwrap_or_else(|_| panic!("unable to seek from device {from} for region id {region_id}"));
            from_dev.read_exact(&mut buf).await.unwrap_or_else(|_| panic!("unable to read from deivce {from} for region id {region_id}"));

            to_dev.seek(smol::io::SeekFrom::Start(offset)).await.unwrap_or_else(|_| panic!("unable to seek to device {to} for region id {region_id}"));
            to_dev.write_all(&buf).await.unwrap_or_else(|_| panic!("unable to write to deivce {to} for region id {region_id}"));
            to_dev.flush().await.unwrap_or_else(|_| panic!("unable to flush to deivce {to} for region id {region_id}"));

            let mut lock = region.lock().await;
            (*lock).state = RecoverState::Clean;
            self.dec_inflight(1);
            drop(lock);

            // break recover process if received cmd is false or channel closed
            match self.rx.try_recv() {
                Ok(cmd) => {
                    if cmd { continue; } else { return; }
                },
                Err(channel::TryRecvError::Empty) => { continue; },
                Err(channel::TryRecvError::Closed) => { return; },
            }
        }

        assert!(self.get_inflight() == 0 && self.get_pending() == 0);
        // all regions recovered, clear recover state bits
        let mut mode_lock = self.mode.write_arc().await;
        let state = self.g_state.clear_all_recover_bits();
        *mode_lock = state;
    }

    // return (inflight, pending, total)
    pub(crate) fn stat(&self) -> (u64, u64, u64) {
        (self.get_inflight(), self.get_pending(), self.nr_regions)
    }

    // vec of region and it's recover state
    pub(crate) fn stat_region_map(&self) -> BTreeMap<u64, RecoverState> {
        let regions = self.region_map.clone();
        let v = smol::block_on(async move {
            let mut map = BTreeMap::new();
            for (id, region) in regions.read_arc().await.iter() {
                let lock = region.lock_arc().await;
                let state = lock.state;
                drop(lock);
                map.insert(*id, state);
            }
            map
        });
        v
    }

    pub(crate) async fn main_loop<T: Replica>(&self, pool: Rc<RefCell<TgtPendingBlocksPool<T>>>, g_region: Rc<region::Region>, task_state: TaskState) {
        task_state.wait_on_tgt_pool_start().await;
        task_state.set_start(TaskId::Recover);
        // keep waiting on next cmd from channel
        while let Ok(cmd) = self.rx.recv().await {
            // dedicate executor for recover workder
            let exec = smol::Executor::new();
            task_state.set_busy(TaskId::Recover);
            if cmd {
                self.do_recovery(pool.clone(), g_region.clone(), task_state.clone(), exec).await;
            }
            task_state.clear_busy(TaskId::Recover);
        }
    }

    pub(crate) fn kickoff(&self) {
        let _ = self.tx.force_send(true);
    }
}

#[inline]
pub(crate) async fn local_recover_ctrl_read(iod: &ublksrv_io_desc) {
    let region_id = region::Region::iod_to_region_id(iod, region::local_region_shift());
    let ctrl = LOCAL_RECOVER_CTRL.with_borrow(|x| x.clone());
    ctrl.q_recover_read(region_id).await;
}

#[inline]
pub(crate) async fn local_recover_ctrl_write(iod: &ublksrv_io_desc) {
    let region_id = region::Region::iod_to_region_id(iod, region::local_region_shift());
    let ctrl = LOCAL_RECOVER_CTRL.with_borrow(|x| x.clone());
    ctrl.q_recover_write(region_id).await;
}
