use std::io::Result;
use std::path::PathBuf;
use clap::Parser;
use bytesize::ByteSize;

#[macro_use]
extern crate nix;

mod utils;
mod device;
mod metadata;
mod ondisk;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[arg(long, short = 'f')]
    pub file: PathBuf,
    /// device size, default is entire primary space
    #[clap(long)]
    pub device_size: Option<String>,
    /// region size
    #[clap(long, default_value = "8MiB")]
    pub region_size: String,
}

fn main() {
    env_logger::init();
    let cli = Cli::parse();

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cli.file)
        .unwrap();

    let (_cap, _sector_bitshift, _pb_bitshift)  = utils::ublk_file_size(&file).unwrap();
    let input_region_size = cli.region_size.parse::<ByteSize>().expect("unable to parse input region size").0;
    let input_device_size = cli.device_size.as_ref()
        .map_or(0, |s| s.parse::<ByteSize>().expect("unable to parse input device size").0);
    let pri_dev = device::PrimaryDevice::new(
        cli.file.as_path().to_str().expect("invalid input of primary device"),
        input_device_size,
        input_region_size
    );
    let meta_dev_desc = device::MetaDeviceDesc::from_primary_device(&pri_dev);
    let _: Result<()> = smol::block_on(async {
        let fake_uuid = uuid::Uuid::new_v4();
        let _ = device::MetaDevice::format(&meta_dev_desc, pri_dev.tgt_device_size, &fake_uuid.as_bytes()).await;
        Ok(())
    });
}
