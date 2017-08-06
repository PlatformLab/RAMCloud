# RAMCloud + NanoLog
[![Build Status](https://travis-ci.org/PlatformLab/RAMCloud.svg?branch=master)](https://travis-ci.org/PlatformLab/RAMCloud)

For up to date information on how to install and use RAMCloud, see the RAMCloud Wiki:
https://ramcloud.stanford.edu/wiki/display/ramcloud

## NanoLog
This branch supplements RAMCloud's custom logger with [NanoLog](https://github.com/PlatformLab/NanoLog), an extremely performant nanosecond scale logging system.

To use NanoLog within RAMCloud, simply invoke NanoLog's printf-like API:
```cpp
#include "Logger.h"
...
NANO_LOG(LogLevel::NOTICE, "Hello World! This is an integer %d and a double %lf\r\n", 1, 2.0);
```

Doing this will cause the RAMCloud system to generate an additional NanoLog log file and a NanoLog Decompressor executable **specific to that compilation** (See Limitations). The NanoLog log file will be in the same location as the RAMCloud log file except with a '.compressed' appended and the Decompressor executable is located in `obj.nanolog/decompressor`

To transform the NanoLog log into a human-readable format, invoke the decompressor:
```bash
obj.nanolog/decompressor ramcloud.log.compressed
```

### Replacing the Built-In RAMCloud Logger
NanoLog can also be configured to replace most of RAMCloud's built-in functionality. More specifically, it can replace all ```RAMCLOUD_LOG``` macros with an invocation to NanoLog's API instead.

To enable this feature, simply compile RAMCloud with the NANOLOG=yes flag:
```bash
make clean-all
make DEBUG=NO NANOLOG=yes -j17
```

Note that not all RAMCloud functionality is supported. Most notably, ```RAMCLOUD_CLOG```, ```RAMCLOUD_BACKTRACE```, ```RAMCLOUD_DIE``` and FileLogger will still forward to RAMCloud's internal logging system.

#### Limitations
- The NanoLog Decompressor executable is *specific to a particular compilation*. Thus, it is advisable to either immediately decompress the logs after the cluster shuts down or save the decompressor application associated with your server/coordinator/backup/client executables with your NanoLog log files. If you use the clusterperf.py script, the latter is automatically done for you.


- NanoLog **requires** a literal format string and a literal LogLevel to be specified IN the ```NANO_LOG``` statement. For example, the following is not allowed:

```cpp
formatStringVar = "Hello World # %d";
LogLevel level = LogLevel::DEBUG;
NANO_LOG(formatStringVar, level, 5); // Compile-Error
```


- A majority of NanoLog's speed-ups come from extracting *static information* from log message. Thus, one should attempt to fit as much information as possible into the format string and avoid logging dynamic strings where possible.

```cpp
NANO_LOG(LogLevel::NOTICE, "This is the best type of log message for NanoLog! It contains no arguments!");
NANO_LOG(LogLevel::NOTICE, "This is a pretty good log message with a dynamic integer %d", number);

NANO_LOG(LogLevel::NOTICE, "Floating points are sub-optimal, but are not bad %lf% of the time", 0.50);
NANO_LOG(LogLevel::NOTICE, "This is the worst because %s", "it contains a long string argument");
```


## What is RAMCloud?
*note*: the following is an excerpt copied from the [RAMCloud wiki](https://ramcloud.stanford.edu/wiki/display/ramcloud)  on 1/22/16.

RAMCloud is a new class of super-high-speed storage for large-scale datacenter applications. It is designed for applications in which a large number of servers in a datacenter need low-latency access to a large durable datastore. RAMCloud offers the following properties:
- **Low Latency**: RAMCloud keeps all data in DRAM at all times, so applications can read RAMCloud objects remotely over a datacenter network in as little as 5μs. Writes take less than 15μs. Unlike systems such as memcached, applications never have to deal with cache misses or wait for disk/flash accesses. As a result, RAMCloud storage is 10-1000x faster than other available alternatives.
- **Large scale**: RAMCloud aggregates the DRAM of thousands of servers to support total capacities of 1PB or more.
- **Durability**: RAMCloud replicates all data on nonvolatile secondary storage such as disk or flash, so no data is lost if servers crash or the power fails. One of RAMCloud's unique features is that it recovers very quickly from server crashes (only 1-2 seconds) so the availability gaps after crashes are almost unnoticeable. As a result, RAMCloud combines the durability of replicated disk with the speed of DRAM. If you have used memcached, you have probably experienced the challenges of managing a second durable storage system and maintaining consistency between it and memcached. With RAMCloud, there is no need for a second storage system.
- **Powerful data model**: RAMCloud's basic data model is a key-value store, but we have extended it with several additional features, such as:
  - Multiple tables, each with its own key space.
  - Transactional updates that span multiple objects in different tables.
  - Secondary indices.
  - Strong consistency: unlike other NoSQL storage systems, all updates in RAMCloud are consistent, immediately visible, and durable.
- **Easy deployment**: RAMCloud is a software package that runs on commodity Intel servers with the Linux operating system. RAMCloud is available freely in open source form.

From a practical standpoint, RAMCloud enables a new class of applications that manipulate large data sets very intensively. Using RAMCloud, an application can combine tens of thousands of items of data in real time to provide instantaneous responses to user requests.  Unlike traditional databases, RAMCloud scales to support very large applications, while still providing a high level of consistency. We believe that RAMCloud, or something like it, will become the primary storage system for structured data in cloud computing environments such as Amazon's AWS or Microsoft's Azure. We have built the system not as a research prototype, but as a production-quality software system, suitable for use by real applications.

RAMCloud is also interesting from a research standpoint. Its two most important attributes are latency and scale. The first goal is to provide the lowest possible end-to-end latency for applications accessing the system from within the same datacenter. We currently achieve latencies of around 5μs for reads and 15μs for writes, but hope to improve these in the future. In addition, the system must scale, since no single machine can store enough DRAM to meet the needs of large-scale applications. We have designed RAMCloud to support at least 10,000 storage servers; the system must automatically manage all the information across the servers, so that clients do not need to deal with any distributed systems issues. The combination of latency and scale has created a large number of interesting research issues, such as how to ensure data durability without sacrificing the latency of reads and writes, how to take advantage of the scale of the system to recover very quickly after crashes, how to manage storage in DRAM, and how to provide higher-level features such as secondary indexes and multiple-object transactions without sacrificing the latency or scalability of the system. Our solutions to these problems are described in a series of technical papers.

The RAMCloud + Nanolog projects are based in the Department of Computer Science at Stanford University.

# Learn More about RAMCloud
https://ramcloud.stanford.edu/wiki/display/ramcloud