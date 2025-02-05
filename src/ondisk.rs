
#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct SuperBlockRaw {
    magic: [u8; 8],
    checksum: u64,
    allocated_size: u64,
    reserved_size: u64,
    metadata_offset: u64,
    creation_timestamp: u64,
    startup_timestamp: u64,
    replica_dev_uuid: [u8; 16],
    padding: [u64; 55],
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct FlushEntryRaw {
    pub cno: u64,
    pub padding: u64,
    pub nanos: u128,
}

impl FlushEntryRaw {
    pub fn is_used(&self) -> bool {
        if self.cno != 0 || self.nanos != 0 {
            return true;
        }
        false
    }

    #[inline]
    pub fn clear(&mut self) {
        self.cno = 0;
        self.nanos = 0;
    }

    #[inline]
    pub fn is_last(index: usize) -> bool {
        if index == 7 { return true; }
        false
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct FlushEntryGroupRaw(pub [FlushEntryRaw; 8]);

impl FlushEntryGroupRaw {
    // find first avail entry, return it's index
    pub fn find_avail(&self) -> Option<usize> {
        let mut index = 0;
        for entry in self.0.iter() {
            if !entry.is_used() {
                return Some(index);
            }
            index += 1;
        }
        None
    }

    pub fn clear_all(&mut self) {
        for mut entry in self.0.iter_mut() {
            entry.clear();
        }
    }
}

// 512 bytes
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct FlushLogBlockRaw {
    pub log_group: [FlushEntryGroupRaw; 2],
}

impl FlushLogBlockRaw {
    pub fn as_mut_u8_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self) as *mut u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub fn as_u8_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_check() {
        assert!(std::mem::size_of::<SuperBlockRaw>() == 512);
        assert!(std::mem::size_of::<FlushLogBlockRaw>() == 512);
    }
}
