use std::fmt;
use std::sync::atomic::Ordering;
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use crate::recover::RecoverState;
use crate::mgmt::Global;
use crate::state;

#[derive(Serialize, Deserialize)]
pub struct PoolStats {
    pub(crate) staging_data_queue_len: usize,
    pub(crate) staging_data_queue_bytes: usize,
    pub(crate) staging_seq_queue_len: usize,
    pub(crate) staging_seq_queue_bytes: usize,
    pub(crate) pending_queue_len: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) inflight_bytes: usize,
    pub(crate) max_capacity: usize,
    pub(crate) l_seq: u64,
    pub(crate) g_seq: u64,
}

impl PoolStats {
    pub(crate) fn is_active(&self) -> bool {
        if self.staging_data_queue_len > 0 || self.staging_data_queue_bytes > 0 ||
            self.staging_seq_queue_len > 0 || self.staging_seq_queue_bytes > 0 ||
            self.pending_queue_len > 0 || self.pending_bytes > 0 || self.inflight_bytes > 0
        {
            return true;
        }
        false
    }
}

impl fmt::Display for PoolStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PoolStats - {}/({}|{}){}/{} inflight/(data|seq)pending/capacity, qlen: ({}|{}){} (data|seq)pending, l_seq: {}, g_seq: {}",
            self.inflight_bytes, self.staging_data_queue_bytes, self.staging_seq_queue_bytes, self.pending_bytes, self.max_capacity,
            self.staging_data_queue_len, self.staging_seq_queue_len, self.pending_queue_len, self.l_seq, self.g_seq
        )
    }
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
        let mask = state::TGT_STATE_RECOVERY_MASK;
        let mut map = if state & mask > 0 {
            self.global.recover.stat_region_map()
        } else {
            BTreeMap::new()
        };
        // patch recover map with dirty region
        for region_id in &region.dirty_ids {
            let _ = map.insert(*region_id, RecoverState::Dirty);
        }
        let v = map.into_iter().map(|(k, v)| (k, v)).collect();
        let recover = RecoverStats {
            inflight: i,
            pending: p,
            nr_regions: n,
            map: v,
        };

        // pool
        let pool = self.global.pool.borrow().get_stats();

        StatsCollection {
            state: state,
            region: region,
            recover: recover,
            pool: pool,
        }
    }
}
