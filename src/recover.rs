use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use smol::channel;
use smol::lock::{Mutex, RwLock};
use libublk::sys::ublksrv_io_desc;
use crate::state::GlobalTgtState;
use crate::state;
use crate::region;
use crate::io_replica::LOCAL_RECOVER_CTRL;

const RecoveryWaitOnMs: u64 = 50;

#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) enum RecoverState {
    NoSync,     // not yet sync
    Recovering, // doing recovery
    Clean,      // in synced
    Dirty,      // new write after synced
}

pub(crate) struct Region {
    pub(crate) id: u64,
    pub(crate) state: RecoverState,
}

#[derive(Clone)]
pub(crate) struct RecoverCtrl {
    primary_path: String,
    replica_path: String,
    region_map: Arc<RwLock<HashMap<u64, Arc<Mutex<Region>>>>>,
    queue: Arc<Mutex<VecDeque<(u64, Arc<Mutex<Region>>)>>>,
    mode: Arc<RwLock<u64>>,
    g_state: GlobalTgtState,
    tx: channel::Sender<bool>,
    rx: channel::Receiver<bool>,
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
            tx: tx,
            rx: rx,
        }
    }
}

impl RecoverCtrl {
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

    // init ctrl with all region in state NoSync
    #[allow(dead_code)]
    pub(crate) fn new(nr_regions: usize, mode: u64, primary: &str, replica: &str) -> Self {
        let mut map = HashMap::new();
        for i in 0..nr_regions as u64 {
            map.insert(i, Arc::new(Mutex::new(Region { id: i, state: RecoverState::NoSync })));
        }
        let mut queue = VecDeque::new();
        for (region_id, region) in map.iter() {
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
            tx: tx,
            rx: rx,
        }
    }

    fn init_no_sync(nr_regions: usize) -> (HashMap<u64, Arc<Mutex<Region>>>, VecDeque<(u64, Arc<Mutex<Region>>)>) {
        let mut map = HashMap::new();
        for i in 0..nr_regions as u64 {
            map.insert(i, Arc::new(Mutex::new(Region { id: i, state: RecoverState::NoSync })));
        }
        let mut queue = VecDeque::new();
        for (region_id, region) in map.iter() {
            queue.push_back((*region_id, region.clone()));
        }
        (map, queue)
    }

    fn rebuild_mode_full(&self, nr_regions: usize, mode: u64) {
        // mode as whole transaction lock
        let mut mode_lock = self.mode.try_write_arc().expect("unable to get write lock for mode");

        // we can now safely change global state
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

        let (region_map, queue) = Self::init_no_sync(nr_regions);

        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = region_map;

        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        // release mode lock
        *mode_lock = mode;
    }

    pub(crate) fn rebuild_mode_reverse_full(&self, nr_regions: usize) {
        self.rebuild_mode_full(nr_regions, state::TGT_STATE_RECOVERY_REVERSE_FULL);
    }

    pub(crate) fn rebuild_mode_forward_full(&self, nr_regions: usize) {
        self.rebuild_mode_full(nr_regions, state::TGT_STATE_RECOVERY_FORWARD_FULL);
    }

    pub(crate) fn rebuild_mode_forward_part(&self, g_region: Rc<region::Region>) {
        // mode as whole transaction lock
        let mut mode_lock = self.mode.try_write_arc().expect("unable to get write lock for mode");

        // we can now safely change global state
        self.g_state.set_recovery_forward_part();

        // get all dirty regions
        let v = g_region.collect();
        // rebuild
        let mut map = HashMap::new();
        let nr_regions = g_region.nr_regions();
        for i in 0..nr_regions {
            map.insert(i, Arc::new(Mutex::new(Region { id: i, state: RecoverState::Clean })));
        }
        // update NoSync region
        let mut nosync = Vec::new();
        for i in v.iter() {
            let region = Arc::new(Mutex::new(Region { id: *i, state: RecoverState::NoSync }));
            map.insert(*i, region.clone());
            nosync.push((*i, region));
        }
        // build queue for NoSync region
        let mut queue = VecDeque::new();
        for (region_id, region) in nosync.iter() {
            queue.push_back((*region_id, region.clone()));
        }

        // update inner
        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = map;

        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        // reset global region and clear dirty flag
        g_region.reset();

        // release mode lock
        *mode_lock = state::TGT_STATE_RECOVERY_FORWARD_PART;
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
    pub(crate) async fn wait_on(region: Arc<Mutex<Region>>) {
        loop {
            let lock = region.lock().await;
            if lock.state == RecoverState::Dirty || lock.state == RecoverState::Clean {
                break;
            }
            drop(lock);
            smol::Timer::after(std::time::Duration::from_millis(RecoveryWaitOnMs)).await;
        }
    }

    // read op run in queue executor context
    pub(crate) async fn q_recover_read(&self, region_id: u64) {
        let mode = *self.mode.read_arc().await;
        if mode == state::TGT_STATE_RECOVERY_FORWARD_FULL
            || mode == state::TGT_STATE_RECOVERY_FORWARD_PART
        {
            // in forward mode, data on primary is updated,
            // no need to wait for read op
            return;
        }

        // in reverse recovery mode
        let r = self.lookup(region_id).await;
        let region = r.lock().await;
        let state = region.state;
        drop(region);
        match state {
            RecoverState::NoSync => {
                // trigger recover of this region in high priority
                self.put_high_prio(region_id).await;
                Self::wait_on(r.clone()).await;
            },
            RecoverState::Recovering => {
                Self::wait_on(r.clone()).await;
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
        // in both forward and reverse mode, write io will be blocked
        let r = self.lookup(region_id).await;
        let region = r.lock().await;
        let state = region.state;
        drop(region);
        match state {
            RecoverState::NoSync => {
                // trigger recover of this region in high priority
                self.put_high_prio(region_id).await;
                Self::wait_on(r.clone()).await;
            },
            RecoverState::Recovering => {
                Self::wait_on(r.clone()).await;
            },
            _ => {
            },
        };
        // handle RecoverState::Clean and RecoverState::Dirty
        let mode = *self.mode.read_arc().await;
        assert!(
            mode == state::TGT_STATE_RECOVERY_FORWARD_FULL
            || mode == state::TGT_STATE_RECOVERY_FORWARD_PART
            || mode == state::TGT_STATE_RECOVERY_REVERSE_FULL
        );
        // in both mode, go ahead to write on primary
        return;
    }

    pub(crate) async fn do_recovery(&self) {
        while let Some((region_id, region)) = self.fetch_one().await {
            let mut lock = region.lock().await;
            (*lock).state = RecoverState::Recovering;
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

            println!("@@@@ copy {} => {} for region id {}", from, to, region_id);

            let mut lock = region.lock().await;
            (*lock).state = RecoverState::Clean;
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

        // all regions recovered, clear recover state bits
        let mut mode_lock = self.mode.write_arc().await;
        let state = self.g_state.clear_all_recover_bits();
        *mode_lock = state;
    }

    pub(crate) async fn main_loop(&self) {
        // keep waiting on next cmd from channel
        while let Ok(cmd) = self.rx.recv().await {
            if cmd {
                self.do_recovery().await;
            }
        }
    }

    pub(crate) fn kickoff(&self) {
        let _ = self.tx.force_send(true);
    }
}

#[inline]
pub(crate) async fn local_recover_ctrl_read(iod: &ublksrv_io_desc) {
    let region_id = region::Region::iod_to_region_id(iod, region::DEFAULT_REGION_SHIFT);
    let ctrl = LOCAL_RECOVER_CTRL.with_borrow(|x| x.clone());
    ctrl.q_recover_read(region_id).await;
}

#[inline]
pub(crate) async fn local_recover_ctrl_write(iod: &ublksrv_io_desc) {
    let region_id = region::Region::iod_to_region_id(iod, region::DEFAULT_REGION_SHIFT);
    let ctrl = LOCAL_RECOVER_CTRL.with_borrow(|x| x.clone());
    ctrl.q_recover_write(region_id).await;
}
