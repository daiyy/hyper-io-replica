use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::collections::HashSet;
use libublk::sys::ublksrv_io_desc;
use crate::io_replica::{LOCAL_DIRTY_REGION, LOCAL_REGION_MAP};

pub const DEFAULT_REGION_SIZE: u64 = 8_388_608;
pub const DEFAULT_REGION_SHIFT: u32 = 23;

#[derive(Clone)]
pub struct Region {
    bitmap: Vec<Arc<AtomicU64>>,
    dirty: Arc<AtomicBool>,
    region_size: u64,
    region_shift: u32,
    dev_size: u64,
    nr_regions: u64,
}

impl fmt::Debug for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Region {{ bitmap vec len: {}, bmap size: {} bytes, is dirty: {}, region size: {} bytes, region shift: {}, regions: {}, device size: {} bytes }}",
            self.bitmap.len(), self.bitmap.len() * 8, self.dirty.load(Ordering::SeqCst),
            self.region_size, self.region_shift, self.nr_regions, self.dev_size
        )
    }
}

impl Region {
    pub fn new(dev_size: u64, region_size: u64) -> Self {
        // check region size aligned to sector size
        let checked_region_size = region_size >> 9 << 9;
        let region_shift = checked_region_size.ilog2();

        let nr_regions = (dev_size + checked_region_size - 1) / checked_region_size;
        let nr_usize = (nr_regions + 64 - 1) / 64;
        let mut v = Vec::new();
        for _ in 0..nr_usize {
            v.push(Arc::new(AtomicU64::new(0)));
        }
        Self {
            bitmap: v,
            dirty: Arc::new(AtomicBool::new(false)),
            region_size: checked_region_size,
            region_shift: region_shift,
            nr_regions: nr_regions,
            dev_size: dev_size,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::SeqCst)
    }

    // convert to region id
    // return: region id
    #[inline]
    pub fn region_id(&self, start: u64, size: u64) -> u64 {
        let region_id = start >> self.region_shift;
        let next_region_id = (start + size) >> self.region_shift;

        // check
        if next_region_id != region_id {
            panic!("cross region access is not allowned - region id {}, next: {}", region_id, next_region_id);
        }
        return region_id;
    }

    pub fn iod_to_region_id(iod: &ublksrv_io_desc, region_shift: u32) -> u64 {
        let start = iod.start_sector << 9;
        let size = (iod.nr_sectors as u64) << 9;

        let region_id = start >> region_shift;
        let next_region_id = (start + size) >> region_shift;

        // FIXME: this check should be remove one day
        if next_region_id != region_id {
            panic!("cross region access is not allowned - region id {}, next: {}", region_id, next_region_id);
        }
        return region_id;
    }

    // return: (bitmap vec index, bit idx)
    // LSB style used - 
    #[inline]
    fn region_id_to_idx(&self, id: u64) -> (usize, usize) {
        let vec_idx = id as usize / 64;
        let bit_idx = id as usize % 64;

        // check
        if vec_idx + 1 > self.bitmap.len() {
            panic!("region id {} exceed region bitmap capacity", id);
        }
        return (vec_idx, bit_idx);
    }

    pub fn mark_dirty(&self, start: u64, size: u64) {
        let region_id = self.region_id(start, size);
        self.mark_dirty_region_id(region_id);
    }

    pub fn mark_dirty_region_id(&self, region_id: u64 ) {
        let (vec_idx, bit_idx) = self.region_id_to_idx(region_id);

        let bits = self.bitmap[vec_idx].clone();
        let dirty_value = 1 << bit_idx;

        let old = bits.fetch_or(dirty_value, Ordering::SeqCst);
        if old == 0 {
            // old value is no dirty, update global dirty bit in anyway
            self.dirty.store(true, Ordering::SeqCst);
        }
    }

    pub fn clear_dirty(&self, start: u64, size: u64) {
        let region_id = self.region_id(start, size);
        self.clear_dirty_region_id(region_id);
    }

    pub fn clear_dirty_region_id(&self, region_id: u64) {
        let (vec_idx, bit_idx) = self.region_id_to_idx(region_id);

        let bits = self.bitmap[vec_idx].clone();
        let dirty_value = !(1 << bit_idx);

        let _ = bits.fetch_and(dirty_value, Ordering::SeqCst);
    }
}

#[inline]
pub(crate) fn local_region_mark_dirty(iod: &ublksrv_io_desc) {
    let region_id = Region::iod_to_region_id(iod, DEFAULT_REGION_SHIFT);
    LOCAL_DIRTY_REGION.with(|set| {
        let _ = set.borrow_mut().insert(region_id);
    })
}

#[inline]
pub(crate) fn local_region_take() -> Vec<u64> {
    LOCAL_DIRTY_REGION.with(|set| {
        let v: Vec<u64> = set.borrow()
            .iter()
            .map(|x| *x)
            .collect();
        *set.borrow_mut() = HashSet::new();
        v
    })
}

#[inline]
pub(crate) fn local_region_map_sync(dirty_region_ids: Vec<u64>) {
    LOCAL_REGION_MAP.with(|map| {
        for region_id in dirty_region_ids {
            map.borrow().mark_dirty_region_id(region_id);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_new() {
        const TB: usize = 1024 * 1024 * 1024 * 1024;
        let dev_size = 1 * TB;
        let region_size = 8 * 1024 * 1024;
        let region = Region::new(dev_size, region_size);
        assert!(region.bitmap.len() == 2048);
        assert!(region.nr_regions == 131072);
    }
}
