use std::fmt;
use std::io::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::collections::HashSet;
use libublk::sys::ublksrv_io_desc;
use crate::io_replica::{LOCAL_DIRTY_REGION, LOCAL_REGION_MAP, LOCAL_REGION_SHIFT};
#[cfg(feature="piopr")]
use crate::io_replica::LOCAL_PIO_PREGION;
#[cfg(feature="piopr")]
use log::{trace, debug, error};

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

    // reset all dirty bits to 0 and clear dirty flag
    pub fn reset(&self) {
        for slot in self.bitmap.iter() {
            slot.store(0, Ordering::SeqCst);
        }
        self.dirty.store(false, Ordering::SeqCst);
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn nr_regions(&self) -> u64 {
        self.nr_regions
    }

    #[inline]
    pub fn region_size(&self) -> u64 {
        self.region_size
    }

    #[inline]
    pub fn region_shift(&self) -> u32 {
        self.region_shift
    }

    // convert to region id
    // return: region id
    #[inline]
    pub fn region_id(&self, start: u64, size: u64) -> u64 {
        Self::to_region_id(start, size, self.region_shift)
    }

    #[inline]
    pub fn to_region_id(start: u64, size: u64, region_shift: u32) -> u64 {
        let region_id = start >> region_shift;
        let next_region_id = (start + size - 1) >> region_shift;

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
        let next_region_id = (start + size - 1) >> region_shift;

        // FIXME: this check should be remove one day
        if next_region_id != region_id {
            panic!("cross region access is not allowned - region id {}, next: {}, region_shift: {}, iod: {:?}",
                region_id, next_region_id, region_shift, iod);
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn clear_dirty(&self, start: u64, size: u64) {
        let region_id = self.region_id(start, size);
        self.clear_dirty_region_id(region_id);
    }

    #[allow(dead_code)]
    pub fn clear_dirty_region_id(&self, region_id: u64) {
        let (vec_idx, bit_idx) = self.region_id_to_idx(region_id);

        let bits = self.bitmap[vec_idx].clone();
        let dirty_value = !(1 << bit_idx);

        let _ = bits.fetch_and(dirty_value, Ordering::SeqCst);
    }

    // find all dirty region from bitmap
    pub fn collect(&self) -> Vec<u64> {
        let mut v = Vec::new();
        for (i, slot) in self.bitmap.iter().enumerate() {
            let bits = slot.load(Ordering::SeqCst);
            if bits == 0 { continue; }
            for shift in 0..64 {
                let mask: u64 = 1 << shift;
                if bits & mask == 0 { continue; }
                let region_id = i as u64 * 64 + shift;
                v.push(region_id);
            }
        }
        if v.len() == 0 {
            assert!(!self.is_dirty());
        }
        v
    }
}

#[inline]
pub(crate) fn local_region_mark_dirty(iod: &ublksrv_io_desc) {
    let region_id = Region::iod_to_region_id(iod, local_region_shift());
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
pub(crate) fn local_region_dirty_count() -> usize {
    LOCAL_DIRTY_REGION.with(|set| {
        set.borrow().len()
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

#[inline]
pub(crate) fn local_region_shift() -> u32 {
    LOCAL_REGION_SHIFT.with(|v| { *(v.borrow()) })
}

#[derive(Clone)]
pub struct PersistRegionMap {
    offset: u64,
    size: u64,
    ptr_value: u64, // value of raw ptr
}

impl fmt::Debug for PersistRegionMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PersistRegionMap {{ offset: {}, size: {}, ptr: {:p}}}",
            self.offset, self.size, self.ptr_value as *const u8)
    }
}

impl PersistRegionMap {
    #[inline]
    fn ptr(ptr_value: u64) -> *mut libc::c_void {
        ptr_value as *mut libc::c_void
    }

    pub(crate) async fn open(dev_path: &str, offset: u64, size: u64) -> Result<Self> {

        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let path = dev_path.to_string();
        let file = smol::unblock(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)
        }).await?;
        let fd = file.as_raw_fd();
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut::<libc::c_void>(),
                size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset as libc::off_t,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        debug!("PersistRegionMap - Open - offset: {offset}, size: {size}, ptr: {ptr:p}");
        Ok(Self {
            offset,
            size,
            ptr_value: ptr as u64,
        })
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.sync().await?;
        let ret = unsafe {
            libc::munmap(Self::ptr(self.ptr_value), self.size as libc::size_t)
        };
        if ret == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        let size = self.size as libc::size_t;
        let ptr_value = self.ptr_value;
        let ret = smol::unblock(move || {
            unsafe {
                libc::msync(Self::ptr(ptr_value), size, libc::MS_SYNC)
            }
        }).await;
        if ret == -1 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    // load persist region map from disk, return vec of dirty region id
    pub(crate) fn load(&self) -> Vec<u64> {
        let mut v = Vec::new();
        let slots = self.size / 8;
        let ptr = Self::ptr(self.ptr_value) as *mut u64;
        for i in 0..slots {
            let bits = unsafe { *ptr.add(i as usize) };
            if bits == 0 { continue; }
            for shift in 0..64 {
                let mask: u64 = 1 << shift;
                if bits & mask == 0 { continue; }
                let region_id = i as u64 * 64 + shift;
                v.push(region_id);
            }
        }
        v
    }

    fn mark_dirty_one(&self, region_id: u64) {
        assert!(region_id <= self.size * 8); // assume region id not exceed persist map space
        let grp_idx = region_id / 64;
        let bit_idx = region_id % 64;

        let ptr = Self::ptr(self.ptr_value) as *mut u64;
        unsafe {
            let ptr = ptr.add(grp_idx as usize);
            let v = *ptr | 1 << bit_idx;
            *ptr = v;
        }
    }

    // mark dirty by region id
    pub(crate) async fn mark_dirty(&self, region_id: u64) -> Result<()> {
        self.mark_dirty_one(region_id);
        self.sync().await
    }

    // mark dirty batch by vec region id
    pub(crate) async fn mark_dirty_batch(&self, regions: Vec<u64>) -> Result<()> {
        for region_id in regions.into_iter() {
            self.mark_dirty_one(region_id)
        }
        self.sync().await
    }

    pub(crate) fn clear_dirty_one(&self, region_id: u64) {
        assert!(region_id < self.size * 8); // assume region id not exceed persist map space
        let grp_idx = region_id / 64;
        let bit_idx = region_id % 64;

        let ptr = Self::ptr(self.ptr_value) as *mut u64;
        unsafe {
            let ptr = ptr.add(grp_idx as usize);
            let v = *ptr & !(1 << bit_idx);
            *ptr = v;
        }
    }

    pub(crate) async fn clear_all(&self) -> Result<()> {
        let slots = self.size / 8;
        let ptr = Self::ptr(self.ptr_value) as *mut u64;
        for i in 0..slots {
            let p_bits = unsafe { ptr.add(i as usize) };
            unsafe { *p_bits = 0; }
        }
        self.sync().await
    }
}

#[cfg(feature="piopr")]
pub(crate) struct PendingIoPersistRegion {
    pending: Arc<AtomicU64>, // pending io count on this region
}

#[cfg(feature="piopr")]
impl PendingIoPersistRegion {
    fn new() -> Self {
        Self {
            pending: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[cfg(feature="piopr")]
pub(crate) struct PendingIoPersistRegionMap {
    mutex: smol::lock::Mutex<()>, // global lock for fields update
    prmap_staging: PersistRegionMap, // backend persist region for staging
    prmap_consist: PersistRegionMap, // backend persist region for consist
    map: Vec<PendingIoPersistRegion>,
    dirty_count: Arc<AtomicU64>, // dirty region count
    region_size: u64,
    region_shift: u32,
    dev_size: u64,
    nr_regions: u64,
}

#[cfg(feature="piopr")]
impl fmt::Display for PendingIoPersistRegionMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "== PendingIoPersistRegionMap - region size: {}, region shift: {}, device size: {}, nr regions: {} ==",
            self.region_size, self.region_shift, self.dev_size, self.nr_regions)?;
        for region_id in 0..self.nr_regions {
            let pending_count = self.map[region_id as usize].pending.load(Ordering::SeqCst);
            if pending_count > 0 {
                writeln!(f, "  region: {region_id} - pending count: {pending_count}")?;
            }
        }
        let _lock = self.mutex.lock_blocking();
        writeln!(f, "  Drity Region in Staging Map {:?}", self.prmap_staging.load())?;
        write!(f, "  Drity Region in Consist Map {:?}", self.prmap_consist.load())
    }
}

#[cfg(feature="piopr")]
impl PendingIoPersistRegionMap {
    pub(crate) fn new(dev_size: u64, region_size: u64, prmap_staging: PersistRegionMap, prmap_consist: PersistRegionMap) -> Self {
        // check region size aligned to sector size
        let checked_region_size = region_size >> 9 << 9;
        let region_shift = checked_region_size.ilog2();

        let nr_regions = (dev_size + checked_region_size - 1) / checked_region_size;
        let map = (0..nr_regions).map(|_| PendingIoPersistRegion::new()).collect();
        Self {
            mutex: smol::lock::Mutex::new(()),
            prmap_staging: prmap_staging,
            prmap_consist: prmap_consist,
            map: map,
            dirty_count: Arc::new(AtomicU64::new(0)),
            region_size: checked_region_size,
            region_shift: region_shift,
            dev_size: dev_size,
            nr_regions: nr_regions,
        }
    }

    pub(crate) fn persist_region_staging(&self, region_id: u64) -> Result<bool> {
        let old_val = self.map[region_id as usize].pending.fetch_add(1, Ordering::SeqCst);
        if old_val == 0 {
            self.dirty_count.fetch_add(1, Ordering::SeqCst);
        }
        if old_val > 0 {
            // if alread dirty, just return
            trace!("PIoRegion - Staging Area - Region: {region_id} already dirty");
            return Ok(false);
        }

        let _lock = self.mutex.lock_blocking();
        smol::block_on(async {
            self.prmap_staging.mark_dirty(region_id).await
        })?;
        trace!("PIoRegion - Staging Area - Region: {region_id} mark dirty");
        Ok(true)
    }

    pub(crate) fn persist_region_consist(&self, region_id: u64, is_flipped: bool) -> Result<()> {
        let old_val = self.map[region_id as usize].pending.fetch_add(1, Ordering::SeqCst);
        if !is_flipped {
            trace!("PIoRegion - Consist Area - Region: {region_id}, is flipped {is_flipped}");
            return Ok(());
        }
        assert!(old_val >= 1);

        let _lock = self.mutex.lock_blocking();
        smol::block_on(async {
            self.prmap_consist.mark_dirty(region_id).await
        })?;
        trace!("PIoRegion - Consist Area - Region: {region_id} mark dirty, is flipped {is_flipped}");
        Ok(())
    }

    // call in io queue write path
    // return:
    //   - true CLEAN->DIRTY flipped
    //   - false already in DIRTY
    pub(crate) fn persist_pending_staging(&self, iod: &ublksrv_io_desc) -> Result<bool> {
        let region_id = Region::iod_to_region_id(iod, self.region_shift);
        self.persist_region_staging(region_id)
    }

    pub(crate) fn persist_pending_consist(&self, iod: &ublksrv_io_desc, is_flipped: bool) -> Result<()> {
        let region_id = Region::iod_to_region_id(iod, self.region_shift);
        self.persist_region_consist(region_id, is_flipped)
    }

    pub(crate) fn handle_primary_io_failed(&self,  iod: &ublksrv_io_desc) {
        let region_id = Region::iod_to_region_id(iod, self.region_shift);
        let old_val = self.map[region_id as usize].pending.fetch_sub(1, Ordering::SeqCst);
        if old_val == 1 {
            self.dirty_count.fetch_sub(1, Ordering::SeqCst);
        }
        if old_val > 1 {
            return;
        }

        let _lock = self.mutex.lock_blocking();
        let res = smol::block_on(async {
            self.prmap_staging.mark_dirty(region_id).await
        });
        if res.is_err() {
            error!("failed to mark priopr staging area dirty");
        }
        let res = smol::block_on(async {
            self.prmap_consist.mark_dirty(region_id).await
        });
        if res.is_err() {
            error!("failed to mark priopr consist area dirty");
        }
    }

    // call in flush context only
    pub(crate) fn dec_pending(&self, v: Vec<crate::pool::PendingIoMeta>) -> Result<()> {
        let _lock = self.mutex.lock_blocking();
        for io in v.into_iter() {
            let region_id = Region::to_region_id(io.offset, io.size as u64, self.region_shift);
            let old_val = self.map[region_id as usize].pending.fetch_sub(2, Ordering::SeqCst);
            if old_val == 2 {
                self.dirty_count.fetch_sub(1, Ordering::SeqCst);
                self.prmap_staging.clear_dirty_one(region_id);
                self.prmap_consist.clear_dirty_one(region_id);
            }
        }
        let res = smol::block_on(async {
            self.prmap_staging.sync().await
        });
        if res.is_err() {
            error!("failed to sync priopr staging area");
            return res;
        }
        let res = smol::block_on(async {
            self.prmap_consist.sync().await
        });
        if res.is_err() {
            error!("failed to sync priopr consist area");
            return res;
        }
        Ok(())
    }
}

#[cfg(feature="piopr")]
#[inline]
pub(crate) fn local_piopr_persist_region_staging(region: u64) -> Result<bool> {
    LOCAL_PIO_PREGION.with_borrow(|map| {
        map.persist_region_staging(region)
    })
}

#[cfg(feature="piopr")]
#[inline]
pub(crate) fn local_piopr_persist_region_consist(region: u64, is_flipped: bool) -> Result<()> {
    LOCAL_PIO_PREGION.with_borrow(|map| {
        map.persist_region_consist(region, is_flipped)
    })
}

#[cfg(feature="piopr")]
#[inline]
pub(crate) fn local_piopr_persist_pending_staging(iod: &ublksrv_io_desc) -> Result<bool> {
    LOCAL_PIO_PREGION.with_borrow(|map| {
        map.persist_pending_staging(iod)
    })
}

#[cfg(feature="piopr")]
#[inline]
pub(crate) fn local_piopr_persist_pending_consist(iod: &ublksrv_io_desc, is_flipped: bool) -> Result<()> {
    LOCAL_PIO_PREGION.with_borrow(|map| {
        map.persist_pending_consist(iod, is_flipped)
    })
}

#[cfg(feature="piopr")]
#[inline]
pub(crate) fn local_piopr_handle_primary_io_failed(iod: &ublksrv_io_desc) {
    LOCAL_PIO_PREGION.with_borrow(|map| {
        map.handle_primary_io_failed(iod)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_new() {
        const TB: u64 = 1024 * 1024 * 1024 * 1024;
        let dev_size = 1 * TB;
        let region_size = 8 * 1024 * 1024;
        let region = Region::new(dev_size, region_size);
        assert!(region.bitmap.len() == 2048);
        assert!(region.nr_regions == 131072);
    }
}
