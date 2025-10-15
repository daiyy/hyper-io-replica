#![allow(dead_code)]
use std::io::Result;
use std::path::PathBuf;
use clap::Parser;
use bytesize::ByteSize;

pub(crate) mod target_flags {
    pub const TGT_QUIET: u64 = 0b00000001;
}

mod args;
mod r#loop;
mod null;
mod io_replica;
mod pool;
mod state;
mod region;
mod recover;
mod mgmt;
mod device;
mod replica;
mod stats;
mod utils;
mod ondisk;
mod metadata;
mod mgmt_proto;
mod mgmt_client;
mod task;
mod seq;

use crate::replica::{Replica, file::FileReplica};
#[cfg(feature="blocking")]
use crate::replica::s3::S3Replica;
#[cfg(feature="reactor")]
use crate::replica::s3_reactor::S3Replica;

const MIN_REGION_SIZE: u64 = 8_388_608; // 8MiB
const MIN_REGION_SHIFT: u32 = 23;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// backing file path of primary device
    #[arg(long, short = 'f')]
    pub file: PathBuf,

    /// device size, default is entire primary space
    #[clap(long)]
    pub device_size: Option<String>,

    /// region size
    #[clap(long, default_value = "8MiB")]
    pub region_size: String,

    /// root uri of replica device
    #[clap(long)]
    pub replica: Option<String>,

    /// meta block size if replica is on S3 and use Hyperfile format
    #[clap(long, default_value_t = 4096)]
    pub meta_block_size: usize,

    /// data block size if replica is on S3 and use Hyperfile format
    #[clap(long, default_value_t = 4096)]
    pub data_block_size: usize,
}

fn main() {
    env_logger::init();
    let cli = Cli::parse();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cli.file)
        .unwrap();

    // handle raw device
    let (cap, sector_bitshift, pb_bitshift)  = utils::ublk_file_size(&file).unwrap();
    log::info!("Raw device: {}, raw size: {}, sector size: {}, physical block size: {}",
        &cli.file.display(), cap, 1 << sector_bitshift, 1 << pb_bitshift);

    // handle region size
    let input_region_size = cli.region_size.parse::<ByteSize>().expect("unable to parse input region size").0;
    if input_region_size % MIN_REGION_SIZE != 0 {
        log::warn!("Input region size not aligned to 8MiB, please adjust the size");
        return;
    }
    let checked_region_size = input_region_size >> MIN_REGION_SHIFT << MIN_REGION_SHIFT;

    // handle device size
    let input_device_size = match cli.device_size {
        Some(s) => s.parse::<ByteSize>().expect("unable to parse input device size").0,
        None => {
            log::warn!("No device size from input, use entire space {}", cap);
            cap
        },
    };
    let checked_device_size = input_device_size >> pb_bitshift << pb_bitshift;

    // create primary device
    let pri_dev = device::PrimaryDevice::new(
        cli.file.as_path().to_str().expect("invalid input of primary device"),
        checked_device_size,
        checked_region_size
    );

    // create meta device desc
    let meta_dev_desc = device::MetaDeviceDesc::from_primary_device(&pri_dev);
    let res: Result<uuid::Uuid> = smol::block_on(async {
        // uuid to link with primay and replica device
        let uuid = uuid::Uuid::new_v4();
        device::MetaDevice::format(&meta_dev_desc, pri_dev.tgt_device_size, &uuid.as_bytes()).await.map(|_| uuid)
    });

    let uuid = match res {
        Ok(uuid) => uuid,
        Err(e) => {
            println!("io-replica format failed, primary device err: {}", e);
            return;
        },
    };

    let Some(input_replica_uri) = cli.replica else {
        println!("io-replica format success, device uuid: {:?}", uuid);
        return;
    };

    // create replica device
    let res = smol::block_on(async {
        let meta_block_size = cli.meta_block_size;
        let data_block_size = cli.data_block_size;
        let checked_replica_uri = if input_replica_uri.starts_with("s3://") || input_replica_uri.starts_with("S3://") {
            // check input meta block size
            if meta_block_size % 4096 != 0 || meta_block_size / 4096 == 0 {
                println!("io-replica format failed, input meta_block_size {meta_block_size} is not a multiple of 4096");
                return Err(anyhow::anyhow!("input data_block_size {meta_block_size} is not a multiple of 4096"));
            };

            // check input data block size
            if data_block_size % 4096 != 0 || data_block_size / 4096 == 1 {
                println!("io-replica format failed, input data_block_size {data_block_size} is not a multiple of 4096");
                return Err(anyhow::anyhow!("input data_block_size {data_block_size} is not a multiple of 4096"));
            };

            // remove tailing '/'
            let replica_root_uri = input_replica_uri.trim_end_matches('/');
            let replica_uri = format!("{}/{:?}", replica_root_uri, uuid);
            S3Replica::create(&replica_uri, pri_dev.tgt_device_size, meta_block_size, data_block_size).await;
            replica_uri
        } else {
            // remove tailing '/'
            let replica_root_uri = input_replica_uri.trim_end_matches('/');
            let replica_uri = format!("{}/{:?}", replica_root_uri, uuid);
            FileReplica::create(&replica_uri, pri_dev.tgt_device_size, meta_block_size, data_block_size).await;
            replica_uri
        };
        Ok(checked_replica_uri)
    });

    match res {
        Ok(replica_uri) => println!("io-replica format success, device uuid: {:?}, replica_uri: {}", uuid, replica_uri),
        Err(e) => println!("io-replica format partial success, device uuid: {:?}, format replica failed {}", uuid, e),
    }
}
