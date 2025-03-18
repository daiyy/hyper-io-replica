use std::fmt;
use std::io::Result;
use std::time::SystemTime;
use log::info;
use smol::fs::{File, OpenOptions, unix::OpenOptionsExt};
use smol::io::{AsyncWriteExt, AsyncReadExt, AsyncSeekExt, SeekFrom};
use block_utils::Device;
use crate::metadata::{SuperBlock, FlushLog};
use crate::ondisk::{SuperBlockRaw, FlushLogBlockRaw};
use crate::region::PersistRegionMap;

const MIN_TARGET_DEVICE_REGIONS: u64 = 2;
const MIN_META_BLOCK_SIZE: u64 = 32_768;

#[derive(Clone, Debug)]
pub struct PrimaryDevice {
    pub device_path: String,
    pub region_size: u64,
    pub tgt_device_size: u64,
    pub reserved_size: u64,
    pub tgt_raw_size: u64,
    #[allow(dead_code)]
    pub back_device: Device,
}

impl PrimaryDevice {
    pub fn new(dev_path: &str, tgt_raw_size: u64, region_size: u64) -> Self {
        // check region size aligned to sector size
        let checked_region_size = region_size >> 9 << 9;

        match block_utils::is_block_device(dev_path) {
            Ok(is_blk) => {
                if !is_blk {
                    panic!("backend device {dev_path} is not a block device");
                }
            },
            Err(e) => {
                panic!("failed to detect backend device {dev_path}, error: {}", e);
            },
        }

        let nr_regions = (tgt_raw_size + checked_region_size - 1) / checked_region_size;
        if nr_regions < MIN_TARGET_DEVICE_REGIONS {
            panic!("raw size of target device is too small, at least {MIN_TARGET_DEVICE_REGIONS} of regions");
        }

        let device = block_utils::get_device_info(dev_path)
            .unwrap_or_else(|_| panic!("failed to get backend device path {dev_path}"));

        if tgt_raw_size > device.capacity {
            panic!("backend deivce {} do NOT have enough space to allocate raw size {} for target device",
                device.capacity, tgt_raw_size);
        }

        let last_region_bytes = tgt_raw_size % checked_region_size;
        let nr_regions_reserved = if last_region_bytes > 0 && last_region_bytes < MIN_META_BLOCK_SIZE {
            // if last region don't have enough space
            2
        } else {
            1
        };

        let nr_regions_tgt = nr_regions - nr_regions_reserved;
        let tgt_device_size = nr_regions_tgt * checked_region_size;
        Self {
            device_path: dev_path.to_string(),
            region_size: checked_region_size,
            tgt_device_size: tgt_device_size,
            reserved_size: tgt_raw_size - tgt_device_size,
            tgt_raw_size: tgt_raw_size,
            back_device: device,
        }
    }

    pub(crate) fn verify(&self) {
    }
}

#[derive(Clone)]
pub struct MetaDeviceDesc {
    pub device_path: String,
    pub offset: u64, // start of meta area, used for flush log
    pub size: u64,
    pub region_map_offset: u64, // offset for dirty region map
    pub region_map_size: u64, // size of dirty region map aligned to 4K block
    pub sb_offset: u64,
}

impl fmt::Display for MetaDeviceDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "MetaDeviceDesc {{device_path: {}, offset: {}, size: {}, sb_offset: {}}}",
            self.device_path, self.offset, self.size, self.sb_offset)
    }
}

impl MetaDeviceDesc {
    pub fn from_primary_device(pri: &PrimaryDevice) -> Self {
        let nr_regions = pri.tgt_device_size / pri.region_size;
        let region_map_size = (nr_regions + 64 - 1) / 64;
        let aligned_region_map_size = (region_map_size + 4096 - 1) / 4096;
        Self {
            device_path: pri.device_path.clone(),
            offset: pri.tgt_device_size,
            size: pri.reserved_size,
            region_map_offset: pri.tgt_device_size + 4096,
            region_map_size: aligned_region_map_size,
            // align sb offset to last sector
            sb_offset: (pri.tgt_raw_size - 512) >> 9 << 9,
        }
    }
}

pub struct MetaDevice {
    pub desc: MetaDeviceDesc,
    pub file: File,
    pub flush_log: FlushLog,
    pub sb: SuperBlock,
    pub preg: PersistRegionMap,
}

impl MetaDevice {
    pub async fn open(desc: &MetaDeviceDesc) -> Self {
        let dev_path = &desc.device_path;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev_path)
            .await
            .unwrap_or_else(|_| panic!("failed to open meta device {dev_path}"));

        // read sb
        let _ = file.seek(SeekFrom::Start(desc.sb_offset)).await;
        info!("load super block from {}", desc.sb_offset);
        let mut sb_raw = SuperBlockRaw::default();
        let _ = file.read_exact(sb_raw.as_mut_u8_slice()).await;

        // read flush log
        let _ = file.seek(SeekFrom::Start(desc.offset)).await;
        info!("load flush log from {}", desc.offset);
        let mut fl_raw = FlushLogBlockRaw::default();
        let _ = file.read_exact(fl_raw.as_mut_u8_slice()).await;

        let preg = PersistRegionMap::open(&dev_path, desc.region_map_offset, desc.region_map_size).await.expect("failed to open persist region map on meta area");

        Self {
            desc: desc.to_owned(),
            file: file,
            flush_log: FlushLog::from(&fl_raw),
            sb: SuperBlock::from(&sb_raw),
            preg: preg,
        }
    }

    #[allow(dead_code)]
    pub async fn format(desc: &MetaDeviceDesc, primary_size: u64, replica_uuid: &[u8; 16]) {
        let dev_path = &desc.device_path;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(dev_path)
            .await
            .unwrap_or_else(|_| panic!("failed to open meta device {dev_path}"));

        // init flush log
        let fl_raw = FlushLogBlockRaw::default();

        // init sb
        let mut sb_raw = SuperBlockRaw::default();
        sb_raw.allocated_size = primary_size;
        sb_raw.reserved_size = desc.size;
        sb_raw.metadata_offset = desc.offset;
        sb_raw.replica_dev_uuid = replica_uuid.to_owned();
        let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        sb_raw.creation_timestamp = now;

        info!("write flush log at {}", desc.offset);
        let _ = file.seek(SeekFrom::Start(desc.offset)).await;
        let res = file.write(fl_raw.as_u8_slice()).await;
        info!("flush log write res: {:?}", res);

        info!("write super block at {}", desc.sb_offset);
        info!("{}", sb_raw);
        let _ = file.seek(SeekFrom::Start(desc.sb_offset)).await;
        let res = file.write(sb_raw.as_u8_slice()).await;
        info!("super block write res: {:?}", res);
    }

    pub async fn flush_log_sync(&mut self, id: u64) {
        self.flush_log.log_one(id);

        let _ = self.file.seek(SeekFrom::Start(self.desc.offset)).await;
        let _ = self.file.write_all(self.flush_log.raw.as_u8_slice()).await;
    }

    pub async fn sb_open_sync(&mut self) {
        let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        self.sb.raw.startup_timestamp = now;
        self.sb.raw.shutdown_timestamp = 0;
        let _ = self.file.seek(SeekFrom::Start(self.desc.sb_offset)).await;
        let _ = self.file.write_all(self.sb.raw.as_u8_slice()).await;
    }

    pub async fn sb_close_sync(&mut self) {
        self.sb.raw.last_cno = self.flush_log.last_entry().cno;

        let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        self.sb.raw.shutdown_timestamp = now;
        let _ = self.file.seek(SeekFrom::Start(self.desc.sb_offset)).await;
        let _ = self.file.write_all(self.sb.raw.as_u8_slice()).await;
    }

    pub fn preg_load(&self) -> Vec<u64> {
        self.preg.load()
    }

    #[allow(dead_code)]
    pub async fn preg_mark_dirty(&self, region_id: u64) -> Result<()> {
        self.preg.mark_dirty(region_id).await
    }

    pub async fn preg_mark_dirty_batch(&self, regions: Vec<u64>) -> Result<()> {
        self.preg.mark_dirty_batch(regions).await
    }

    pub async fn preg_clear_all(&self) -> Result<()> {
        self.preg.clear_all().await
    }
}
