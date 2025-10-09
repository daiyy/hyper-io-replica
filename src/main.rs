use args::{AddCommands, Commands};
use clap::Parser;
use libublk::{ctrl::UblkCtrl, UblkFlags};
use std::path::Path;
use std::sync::Arc;

pub(crate) mod target_flags {
    pub const TGT_QUIET: u64 = 0b00000001;
}

#[macro_use]
extern crate nix;

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

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn ublk_dump_dev(comm: &Arc<crate::utils::DevIdComm>) -> anyhow::Result<i32> {
    match comm.recieve_dev_id() {
        Ok(id) => {
            UblkCtrl::new_simple(id).unwrap().dump();
            Ok(0)
        }
        _ => Err(anyhow::anyhow!("not recieved device id")),
    }
}

/// Wait until control device state is updated to `state`
fn ublk_state_wait_until(ctrl: &mut UblkCtrl, state: u32, timeout: u32) -> anyhow::Result<i32> {
    let mut count = 0;
    let unit = 100_u32;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(unit as u64));

        ctrl.read_dev_info()?;
        if ctrl.dev_info().state == state as u16 {
            return Ok(0);
        }
        count += unit;
        if count >= timeout {
            return Err(anyhow::anyhow!("timeout error"));
        }
    }
}

fn ublk_parse_add_args(opt: &args::AddCommands) -> (&'static str, &args::GenAddArgs) {
    match opt {
        AddCommands::Loop(_opt) => ("loop", &_opt.gen_arg),
        AddCommands::Null(_opt) => ("null", &_opt.gen_arg),
        AddCommands::IoReplica(_opt) => ("io-replica", &_opt.gen_arg),
    }
}

fn ublk_add_worker(opt: args::AddCommands, comm: &Arc<crate::utils::DevIdComm>) -> anyhow::Result<i32> {
    let (tgt_type, gen_arg) = ublk_parse_add_args(&opt);
    let ctrl = gen_arg.new_ublk_ctrl(tgt_type, UblkFlags::UBLK_DEV_F_ADD_DEV)?;

    // Validate mlock compatibility early
    if let Err(e) = gen_arg.validate_mlock_compatibility(tgt_type) {
        comm.send_dev_id(ctrl.dev_info().dev_id)?;
        return Err(e);
    }

    match opt {
        AddCommands::Loop(opt) => r#loop::ublk_add_loop(ctrl, Some(opt), comm),
        AddCommands::Null(opt) => null::ublk_add_null(ctrl, Some(opt), comm),
        AddCommands::IoReplica(opt) => io_replica::ublk_add_io_replica(ctrl, Some(opt), comm),
    }
}

fn ublk_add(opt: args::AddCommands) -> anyhow::Result<i32> {
    let (_, gen_arg) = ublk_parse_add_args(&opt);
    let comm = Arc::new(crate::utils::DevIdComm::new(gen_arg.foreground).expect("Create eventfd failed"));
    gen_arg.save_start_dir();

    if gen_arg.foreground {
        ublk_add_worker(opt, &comm)
    } else {
        let daemonize = daemonize::Daemonize::new()
            .stdout(daemonize::Stdio::devnull())
            .stderr(daemonize::Stdio::devnull());

        match daemonize.execute() {
            daemonize::Outcome::Child(Ok(_)) => match ublk_add_worker(opt, &comm) {
                Ok(res) => Ok(res),
                Err(r) => {
                    comm.write_failure().expect("fail to send failure");
                    Err(r)
                }
            },
            daemonize::Outcome::Parent(Ok(_)) => ublk_dump_dev(&comm),
            _ => Err(anyhow::anyhow!("daemonize execute failure")),
        }
    }
}

fn ublk_recover_work(opt: args::UblkArgs) -> anyhow::Result<i32> {
    if opt.number < 0 {
        return Err(anyhow::anyhow!("invalid device number"));
    }

    let ctrl = UblkCtrl::new_simple(opt.number)?;

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64)) == 0 {
        return Err(anyhow::anyhow!("not set user recovery flag"));
    }

    if ctrl.dev_info().state != libublk::sys::UBLK_S_DEV_QUIESCED as u16 {
        return Err(anyhow::anyhow!("device isn't quiesced"));
    }

    let comm = Arc::new(crate::utils::DevIdComm::new(false).expect("Create eventfd failed"));
    ctrl.start_user_recover()?;

    let tgt_type = ctrl.get_target_type_from_json().unwrap();

    // Extract device info and target flags to restore original configuration
    let dev_info = ctrl.dev_info();
    let target_flags = dev_info.ublksrv_flags;

    // Restore original dev_flags from high 32 bits of target_flags
    let stored_dev_flags_bits = (target_flags >> 32) as u32;
    let recovered_dev_flags =
        UblkFlags::from_bits_truncate(stored_dev_flags_bits) | UblkFlags::UBLK_DEV_F_RECOVER_DEV;

    let ctrl = libublk::ctrl::UblkCtrlBuilder::default()
        .name(&tgt_type.clone())
        .depth(dev_info.queue_depth)
        .nr_queues(dev_info.nr_hw_queues)
        .id(dev_info.dev_id as i32)
        .ctrl_flags(libublk::sys::UBLK_F_USER_RECOVERY.into())
        .dev_flags(recovered_dev_flags)
        .build()
        .unwrap();

    match tgt_type.as_str() {
        "loop" => r#loop::ublk_add_loop(ctrl, None, &comm),
        "null" => null::ublk_add_null(ctrl, None, &comm),
        "io-replica" => io_replica::ublk_add_io_replica(ctrl, None, &comm),
        &_ => Err(anyhow::anyhow!("unsupported target type: {}", tgt_type)),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> anyhow::Result<i32> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::devnull())
        .stderr(daemonize::Stdio::devnull());

    let id = opt.number;
    if id < 0 {
        return Err(anyhow::anyhow!("invalid device id"));
    }

    match daemonize.execute() {
        daemonize::Outcome::Child(Ok(_)) => ublk_recover_work(opt),
        daemonize::Outcome::Parent(Ok(_)) => {
            let mut ctrl = UblkCtrl::new_simple(id)?;
            ublk_state_wait_until(&mut ctrl, libublk::sys::UBLK_S_DEV_LIVE, 5000)?;

            if (ctrl.dev_info().ublksrv_flags & target_flags::TGT_QUIET) == 0 {
                ctrl.dump();
            }
            Ok(0)
        }
        _ => Err(anyhow::anyhow!("daemonize execute failed")),
    }
}

const FEATURES_TABLE: &[&str] = &[
    "ZERO_COPY",
    "COMP_IN_TASK",
    "NEED_GET_DATA",
    "USER_RECOVERY",
    "USER_RECOVERY_REISSUE",
    "UNPRIVILEGED_DEV",
    "CMD_IOCTL_ENCODE",
    "USER_COPY",
    "ZONED",
    "USER_RECOVERY_FAIL_IO",
    "UPDATE_SIZE",
    "AUTO_BUF_REG",
    "QUIESCE",
    "PER_IO_DAEMON",
    "BUF_REG_OFF_DAEMON",
];

#[allow(clippy::needless_range_loop)]
fn ublk_features(_opt: args::UblkFeaturesArgs) -> anyhow::Result<i32> {
    match UblkCtrl::get_features() {
        Some(f) => {
            println!("\t{:<22} {:#12x}", "UBLK FEATURES", f);
            for i in 0..64 {
                if ((1_u64 << i) & f) == 0 {
                    continue;
                }

                let feat = if i < FEATURES_TABLE.len() {
                    FEATURES_TABLE[i]
                } else {
                    "unknown"
                };
                println!("\t{:<22} {:#12x}", feat, 1_u64 << i);
            }
        }
        None => eprintln!("not support GET_FEATURES, require linux v6.5"),
    }
    Ok(0)
}

fn __ublk_del(id: i32, async_del: bool) -> anyhow::Result<i32> {
    let ctrl = UblkCtrl::new_simple(id)?;

    ctrl.kill_dev()?;
    match async_del {
        false => ctrl.del_dev()?,
        true => ctrl.del_dev_async()?,
    };

    let run_path = ctrl.run_path();
    let json_path = std::path::Path::new(&run_path);
    assert!(!json_path.exists());

    Ok(0)
}

fn ublk_del(opt: args::DelArgs) -> anyhow::Result<i32> {
    log::trace!("ublk del {} {}", opt.number, opt.all);

    if !opt.all {
        __ublk_del(opt.number, opt.r#async)?;

        return Ok(0);
    }

    UblkCtrl::for_each_dev_id(move |id| {
        if let Err(e) = __ublk_del(id.try_into().unwrap(), opt.r#async) {
            eprintln!("failed to delete ublk device {}: {}", id, e);
        }
    });

    Ok(0)
}

fn __ublk_list(id: i32) -> anyhow::Result<i32> {
    match UblkCtrl::new_simple(id) {
        Ok(ctrl) => {
            if ctrl.read_dev_info().is_ok() {
                ctrl.dump();
            }
            Ok(0)
        }
        _ => Err(anyhow::anyhow!("nodev failure")),
    }
}

fn ublk_list(opt: args::UblkArgs) -> anyhow::Result<i32> {
    if opt.number >= 0 {
        __ublk_list(opt.number)?;
        return Ok(0);
    }

    UblkCtrl::for_each_dev_id(move |id| {
        if let Err(e) = __ublk_list(id.try_into().unwrap()) {
            eprintln!("failed to list ublk device {}: {}", id, e);
        }
    });

    Ok(0)
}

fn main() {
    let cli = Cli::parse();

    env_logger::builder()
        .format_target(false)
        .format_timestamp(None)
        .init();

    if !Path::new("/dev/ublk-control").exists() {
        eprintln!("Please run `modprobe ublk_drv` first");
        std::process::exit(1);
    }

    match cli.command {
        Commands::Add(opt) => {
            ublk_add(opt).expect("Fail to add ublk, pass --foreground for getting detailed error\n")
        }
        Commands::Del(opt) => ublk_del(opt).unwrap(),
        Commands::List(opt) => ublk_list(opt).unwrap(),
        Commands::Recover(opt) => ublk_recover(opt).unwrap(),
        Commands::Features(opt) => ublk_features(opt).unwrap(),
    };
}
