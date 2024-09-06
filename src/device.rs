use block_utils::Device;

const MIN_TARGET_DEVICE_REGIONS: u64 = 2;
const MIN_META_BLOCK_SIZE: u64 = 32_768;

#[derive(Clone, Debug)]
pub struct PrimaryDevice {
    pub device_path: String,
    pub region_size: u64,
    pub tgt_device_size: u64,
    pub reserved_size: u64,
    pub tgt_raw_size: u64,
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

    pub fn verify(&self) {
    }
}
