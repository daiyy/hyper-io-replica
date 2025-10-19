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

## Under Developement

:warning: This library is currently under developement and is **NOT** recommended for production.

## License

This library is licensed under the GPLv2 or later License. See the [LICENSE](LICENSE) file.
