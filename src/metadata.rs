use std::time::SystemTime;
use crate::ondisk::*;

#[derive(Debug, Default)]
pub struct FlushLog {
    pub raw: FlushLogBlockRaw,
    pub current_group: usize,
    pub current_index: usize,
}

impl FlushLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from(raw: &FlushLogBlockRaw) -> Self {
        let mut log = Self {
            raw: *raw,
            current_group: 0,
            current_index: 0,
        };
        log.define_seq();
        log
    }

    pub fn define_seq(&mut self) {
        let grp0 = self.raw.log_group[0].find_avail();
        let grp1 = self.raw.log_group[1].find_avail();

        if grp0.is_none() && grp1.is_none() {
            panic!("inconsistent of FlushLogBlock, both group0 and group1 entries are full");
        }

        if grp0.is_some() && grp1.is_none() {
            self.current_group = 0;
            self.current_index = grp0.unwrap();
            return;
        }

        if grp1.is_some() && grp0.is_none() {
            self.current_group = 1;
            self.current_index = grp1.unwrap();
            return;
        }

        // if both group0 and group1 have available log entry
        let grp0_idx = grp0.unwrap();
        let grp1_idx = grp1.unwrap();

        if grp0_idx == 0 && grp1_idx == 0 {
            // let's start from group0/index0
            self.current_group = 0;
            self.current_index = 0;
            return;
        }

        if grp0_idx == 0 && grp1_idx > 0 {
            self.current_group = 1;
            self.current_index = grp1_idx;
            return;
        }

        if grp1_idx == 0 && grp0_idx > 0 {
            self.current_group = 0;
            self.current_index = grp0_idx;
            return;
        }

        panic!("inconsistent of FlushLogBlock, at least one log group should be full or empty");
    }

    pub fn log_one(&mut self, cno: u64) {
        let mut entry = &mut self.raw.log_group[self.current_group].0[self.current_index];
        assert!(!entry.is_used());

        let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
        entry.cno = cno;
        entry.nanos = now;

        if FlushEntryRaw::is_last(self.current_index) {
            let sib_group = if self.current_group == 0 { 1 } else { 0 };
            self.raw.log_group[sib_group].clear_all();
            self.current_group = sib_group;
            self.current_index = 0;
            return;
        }
        // goto next entry of this group
        self.current_index += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_x(x: u64) {
        let mut flush_log = FlushLog::new();
        for i in 1..=x {
            flush_log.log_one(i);
        }
        let raw = flush_log.raw;
        let group = flush_log.current_group;
        let index = flush_log.current_index;

        let mut flush_log = FlushLog::from(&raw);
        assert!(group == flush_log.current_group);
        assert!(index == flush_log.current_index);

    }

    #[test]
    fn flush_log_test() {
        for x in 1000..=1020 {
            log_x(x as u64);
        }
    }
}
