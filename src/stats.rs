use std::sync::atomic::Ordering;
use serde::{Serialize, Deserialize};
use crate::recover::RecoverState;
use crate::mgmt::Global;
use crate::state;

#[derive(Serialize, Deserialize)]
pub struct PoolStats {
    pending_bytes: usize,
    max_capacity: usize,
}

#[derive(Serialize, Deserialize)]
pub struct RegionStats {
    dirty_ids: Vec<u64>,
    dirty: bool,
    region_size: u64,
    region_shift: u32,
    nr_regions: u64,
}

#[derive(Serialize, Deserialize)]
pub struct RecoverStats {
    inflight: u64,
    pending: u64,
    nr_regions: u64,
    map: Vec<(u64, RecoverState)>,
}

#[derive(Serialize, Deserialize)]
pub struct StatsCollection {
    state: u64,
    region: RegionStats,
    recover: RecoverStats,
    pool: PoolStats,
}

pub(crate) struct Stats<T> {
    global: Global<T>,
}

impl<T> Stats<T> {
    pub(crate) fn new(global: Global<T>) -> Self {
        Self { global }
    }

    pub(crate) fn collect(&self) -> StatsCollection {
        // state
        let state = self.global.state.state_clone().load(Ordering::SeqCst);

        // region
        let region = RegionStats {
            dirty_ids: self.global.region.collect(),
            dirty: self.global.region.is_dirty(),
            region_size: self.global.region.region_size(),
            region_shift: self.global.region.region_shift(),
            nr_regions: self.global.region.nr_regions(),
        };

        // recover
        let (i, p, n) = self.global.recover.stat();
        let mask = state::TGT_STATE_RECOVERY_FORWARD_FULL |
            state::TGT_STATE_RECOVERY_FORWARD_PART |
            state::TGT_STATE_RECOVERY_REVERSE_FULL;
        let map = if state & mask > 0 {
            self.global.recover.stat_region_map()
        } else {
            Vec::new()
        };
        let recover = RecoverStats {
            inflight: i,
            pending: p,
            nr_regions: n,
            map: map,
        };

        // pool
        let pool = self.global.pool.borrow();
        let (p, m) = pool.get_stats();
        let pool = PoolStats {
            pending_bytes: p,
            max_capacity: m,
        };

        StatsCollection {
            state: state,
            region: region,
            recover: recover,
            pool: pool,
        }
    }
}
