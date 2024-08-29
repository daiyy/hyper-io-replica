use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use smol::lock::{Mutex, RwLock};
use libublk::sys::ublksrv_io_desc;
use crate::state;
use crate::region;
use crate::io_replica::LOCAL_RECOVER_CTRL;

#[derive(PartialEq)]
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
    region_map: Arc<RwLock<HashMap<u64, Arc<Mutex<Region>>>>>,
    queue: Arc<Mutex<VecDeque<(u64, Arc<Mutex<Region>>)>>>,
    mode: Arc<RwLock<u64>>,
}

impl Default for RecoverCtrl {
    fn default() -> Self {
        Self {
            region_map: Arc::new(RwLock::new(HashMap::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            mode: Arc::new(RwLock::new(state::TGT_STATE_LOGGING_ENABLED)),
        }
    }
}

impl RecoverCtrl {
    // init ctrl with all region in state NoSync
    #[allow(dead_code)]
    pub(crate) fn new(nr_regions: usize, mode: u64) -> Self {
        let mut map = HashMap::new();
        for i in 0..nr_regions as u64 {
            map.insert(i, Arc::new(Mutex::new(Region { id: i, state: RecoverState::NoSync })));
        }
        let mut queue = VecDeque::new();
        for (region_id, region) in map.iter() {
            queue.push_back((*region_id, region.clone()));
        }
        Self {
            region_map: Arc::new(RwLock::new(map)),
            queue: Arc::new(Mutex::new(queue)),
            mode: Arc::new(RwLock::new(mode)),
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
        let (region_map, queue) = Self::init_no_sync(nr_regions);

        let mut lock = self.region_map.try_write_arc().expect("unable to get write lock for region_map");
        *lock = region_map;

        let mut lock = self.queue.try_lock_arc().expect("unable to get lock for prio queue");
        *lock = queue;

        let mut lock = self.mode.try_write_arc().expect("unable to get write lock for mode");
        *lock = mode;
    }

    pub(crate) fn rebuild_mode_reverse_full(&self, nr_regions: usize) {
        self.rebuild_mode_full(nr_regions, state::TGT_STATE_RECOVERY_REVERSE_FULL);
    }

    pub(crate) fn rebuild_mode_forward_full(&self, nr_regions: usize) {
        self.rebuild_mode_full(nr_regions, state::TGT_STATE_RECOVERY_FORWARD_FULL);
    }

    pub(crate) fn rebuild_mode_forward_part(&self, nr_regions: usize, g_region: Rc<region::Region>) {
        // get all dirty regions
        let v = g_region.collect();
        // rebuild
        let mut map = HashMap::new();
        for i in 0..nr_regions as u64 {
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

        let mut lock = self.mode.try_write_arc().expect("unable to get write lock for mode");
        *lock = state::TGT_STATE_RECOVERY_FORWARD_PART;

        // reset global region and clear dirty flag
        g_region.reset();
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
        assert!(region.state != RecoverState::Recovering);
        if region.state == RecoverState::NoSync {
            // trigger recover of this region in high priority
            self.put_high_prio(region_id).await;
            return;
        }
        // handle RecoverState::Clean and RecoverState::Dirty
        // region reverse recovery is done, let do read
        return;
    }

    // write op run in queue executor context
    pub(crate) async fn q_recover_write(&self, region_id: u64) {
        // in both forward and reverse mode, write io will be blocked
        let r = self.lookup(region_id).await;
        let region = r.lock().await;
        assert!(region.state != RecoverState::Recovering);
        if region.state == RecoverState::NoSync {
            // trigger recover of this region in high priority
            self.put_high_prio(region_id).await;
            return;
        }
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

    pub(crate) fn do_recovery(&self) {
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
