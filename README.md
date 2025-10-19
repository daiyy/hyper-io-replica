# hyper-io-replica

Asynchronous IO replicator for block device (aka. tinylag).

## Overview

As NVMe based block device get more and more faster, the cost of providing a fully synchronized block deivce copy becomes expensive.

This project implements yet anther block device replication framework to replicate data between source and target device in asynchronous manner, to provide **most closer** snapshot for source device.

## Terminology

- **Primary Device** - source block device of replication.
- **Replica Device** - target block device of replication.
- **Memory Pool** - host memory space reserved for holding IOs before written to replica device.
- **IO Lag** - temporarily data copy of IO in memory pool that not yet been written to replica device.
- **Region** - granularity of block device address space to track consistency between primary and replica deivce.

## Replication Mode

| Mode | IO Direction | Memory Pool State | Granularity |
| ---- | ---- | ---- | ---- |
| Logging Enabled | Primary -> Replica | Available | IO |
| Forward Part | Primary -> Repclia | Unavailable | Region |
| Forward Full | Primary -> Repclia | Unavailable | Region |
| Reserve Full | Replica -> Primary | Unavailable | Region |

## Features

- Uerspace implementation based on [ublk](https://docs.kernel.org/block/ublk.html)
- S3 as Replica device 
- Continuous data protection on Replica device
- Consistency tracking/checking between Primary and Replica
- Restore data from checkpoint on Replica in lazy load style

## How to run

### Format Primary/Replica Device

Before using hyper-io-replica, you need to format Primary and Replica device with one single command:

```
# hyper-io-replica device size: 4G
# Primary device path: /dev/nvme1n1
# Replica device url: s3://mybucket/replica_device_root

# example for format both Primary and Replica device:
$ cargo run --release --bin format -- --file /dev/nvmen1n1 --device-size 4G --replica "s3://mybucket/replica_device_root"
```

One can also format Primary device only and format(create) Replica device with [hypercli](https://github.com/hyperfile/hypercli):

```
# hyper-io-replica device size: 4G
# Primary device path: /dev/nvme1n1
# Replica device url: s3://mybucket/replica_device_root

# example to format Primary device only:
$ cargo run --release --bin format -- --file /dev/nvmen1n1 --device-size 4G

# example to format Replica device with hypercli:
$ hypercli file create "s3://mybucket/replica_device_root"
```

After format done, you will get output of device UUID like below:

```
io-replica format success, device uuid: 2859cfc3-2019-4b5c-bf05-0f70baf1b950, replica_uri: s3://mybucket/replica_device_root/2859cfc3-2019-4b5c-bf05-0f70baf1b950
```

See help for more format options:

```
$ cargo run --release --bin format -- -h
    Finished `release` profile [optimized] target(s) in 0.17s
     Running `target/release/format -h`
Usage: format [OPTIONS] --file <FILE>

Options:
  -f, --file <FILE>
          backing file path of primary device
      --device-size <DEVICE_SIZE>
          device size, default is entire primary space
      --region-size <REGION_SIZE>
          region size [default: 8MiB]
      --replica <REPLICA>
          root uri of replica device
      --meta-block-size <META_BLOCK_SIZE>
          meta block size if replica is on S3 and use Hyperfile format [default: 4096]
      --data-block-size <DATA_BLOCK_SIZE>
          data block size if replica is on S3 and use Hyperfile format [default: 4096]
  -h, --help
          Print help
  -V, --version
          Print version
```

### Run IO Replica device

Run io-replica device typically with following command with UUID output from format command:

```
$ cargo run --release --bin hyper-io-replica -- add io-replica --unprivileged --file /dev/nvme1n1 --local-pool-size 64MiB --replica s3://mybucket/replica_device_root/2859cfc3-2019-4b5c-bf05-0f70baf1b950 -q 4 --unix-socket /tmp/.ioreplica --foreground --device-size 4G --recover-concurrency 4 --force-startup-recover --pool-size 512MiB --uuid 2859cfc3-2019-4b5c-bf05-0f70baf1b950
```

A ublk block device will then be created in OS as `/dev/ublkb*`.

hyper-io-replica is based on [rublk](https://github.com/ublk-org/rublk) and [libublk](https://github.com/ublk-org/libublk-rs).

See also cli help for more hyper-io-replica device startup options:

```
$ cargo run --release --bin hyper-io-replica -- add io-replica -h
    Finished `release` profile [optimized] target(s) in 0.16s
     Running `target/release/hyper-io-replica add io-replica -h`
Add io-replica target

Usage: hyper-io-replica add io-replica [OPTIONS] --file <FILE> --replica <REPLICA> --uuid <UUID>

Options:
  -n, --number <NUMBER>
          device id, -1 means ublk driver assigns ID for us [default: -1]
  -q, --queue <QUEUE>
          nr_hw_queues [default: 1]
  -d, --depth <DEPTH>
          queue depth [default: 128]
  -b, --io-buf-size <IO_BUF_SIZE>
          io buffer size, has to be aligned with PAGE_SIZE
          and common suffixes supported ([B|KiB|MiB|GiB]) [default: 524288]
  -r, --user-recovery
          enable user recovery
  -u, --user-copy
          enable user copy
  -p, --unprivileged
          enable unprivileged
      --quiet
          quiet, don't dump device info
      --logical-block-size <LOGICAL_BLOCK_SIZE>
          logical block size
      --physical-block-size <PHYSICAL_BLOCK_SIZE>
          physical block size
  -o, --read-only
          read only
      --foreground
          start the device in foreground
  -z, --zero-copy
          Use auto_buf_reg for supporting zero copy
      --multi-cpus-affinity
          Use multi-cpus affinity instead of single CPU affinity (default is single CPU)
      --mlock
          Enable memory locking for I/O buffers (sets UBLK_DEV_F_MLOCK_IO_BUFFER)
  -f, --file <FILE>
          backing file of ublk target
      --buffered-io
          buffered io is applied for backing file of ublk target, default is direct IO
  -a, --async-await
          use async_await
      --replica <REPLICA>
          replica device
      --region-size <REGION_SIZE>
          region size [default: 8MiB]
      --pool-size <POOL_SIZE>
          pool size [default: 1GiB]
      --local-pool-size <LOCAL_POOL_SIZE>
          local pool size, default is disable local pool [default: 0]
      --device-size <DEVICE_SIZE>
          device size, default is entire primary space
      --unix-socket <UNIX_SOCKET>
          unix socket for management command channel [default: ./.ioreplica]
      --force-startup-recover
          force recover during startup
      --recover-concurrency <RECOVER_CONCURRENCY>
          recover concurrency [default: 1]
      --uuid <UUID>
          device UUID
  -h, --help
          Print help
```

## Under Developement

:warning: This library is currently under developement and is **NOT** recommended for production.

## License

This library is licensed under the GPLv2 or later License. See the [LICENSE](LICENSE) file.
