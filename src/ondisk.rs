use std::fmt;
use time::format_description::well_known::Rfc3339;

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct SuperBlockRaw {
    pub magic: [u8; 8],
    pub checksum: u64,
    pub allocated_size: u64,
    pub reserved_size: u64,
    pub metadata_offset: u64,
    pub persist_region_map_offset: u64,
    pub persist_region_map2_offset: u64,
    pub persist_region_map_size: u64,
    pub creation_timestamp: u64,
    pub startup_timestamp: u64,
    pub shutdown_timestamp: u64, // timestamp is 0 means not a clean shutdown
    pub last_cno: u64,
    pub replica_dev_uuid: [u8; 16],
    padding: [u64; 50],
}

impl fmt::Display for SuperBlockRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::borrow::Cow;
        use std::ffi::CStr;
        use uuid::Uuid;

        let magic = CStr::from_bytes_with_nul(&self.magic)
                .and_then(
                    |s| Ok(s.to_string_lossy())
                )
                .unwrap_or(
                    Cow::Borrowed("Invalid SuperBlock Magic")
                );
        let replica_uuid = Uuid::from_slice(&self.replica_dev_uuid).unwrap();

        let create = time::OffsetDateTime::from_unix_timestamp(self.creation_timestamp as i64).unwrap();
        let startup = time::OffsetDateTime::from_unix_timestamp(self.startup_timestamp as i64).unwrap();
        let shutdown = time::OffsetDateTime::from_unix_timestamp(self.shutdown_timestamp as i64).unwrap();

        writeln!(f, "  magic: {}, replica: {}", &magic, replica_uuid)?;
        writeln!(f, "  checksum: {:x}, last cno: {}", self.checksum, self.last_cno)?;
        writeln!(f, "  allocated_size: {}, reserved_size: {}, metadata_offset: {}",
            self.allocated_size, self.reserved_size, self.metadata_offset)?;
        writeln!(f, "  persist region map offset: {}, map2 offset: {}, map size: {}",
            self.persist_region_map_offset, self.persist_region_map2_offset, self.persist_region_map_size)?;
        write!(f, "  creation_time: {}, startup_time: {}, shutdown_time: {}",
            create.format(&Rfc3339).unwrap(),
            startup.format(&Rfc3339).unwrap(),
            shutdown.format(&Rfc3339).unwrap())
    }
}

impl Default for SuperBlockRaw {
    fn default() -> Self {
        Self {
            magic: [b'H', b'Y', b'P', b'E', b'R', b'I', b'O', 0],
            checksum: 0,
            allocated_size: 0,
            reserved_size: 0,
            metadata_offset: 0,
            persist_region_map_offset: 0,
            persist_region_map2_offset: 0,
            persist_region_map_size: 0,
            creation_timestamp: 0,
            startup_timestamp: 0,
            shutdown_timestamp: 0,
            last_cno: 0,
            replica_dev_uuid: [0u8; 16],
            padding: [0u64; 50],
        }
    }
}

impl SuperBlockRaw {
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

impl fmt::Display for FlushEntryRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.cno == 0 && self.nanos == 0 {
            write!(f, "[Unused Entry]")?;
            return Ok(());
        }
        let dt = time::OffsetDateTime::from_unix_timestamp_nanos(self.nanos as i128).unwrap();
        write!(f, "[{}, {}]", self.cno, dt.format(&Rfc3339).unwrap())
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct FlushEntryGroupRaw(pub [FlushEntryRaw; 8]);

impl fmt::Display for FlushEntryGroupRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, entry) in self.0.iter().enumerate() {
            write!(f, "[{}]{} ", i, entry)?;
        }
        Ok(())
    }
}

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
        for entry in self.0.iter_mut() {
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

impl fmt::Display for FlushLogBlockRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "group0 => {}", self.log_group[0])?;
        write!(f, "group1 => {}", self.log_group[1])
    }
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
