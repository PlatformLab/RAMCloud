# RAMCloud
[![Build Status](https://travis-ci.org/PlatformLab/RAMCloud.svg?branch=master)](https://travis-ci.org/PlatformLab/RAMCloud)

For up to date information on how to install and use RAMCloud, see the RAMCloud Wiki:
https://ramcloud.atlassian.net/wiki/spaces/RAM/overview

# What is RAMCloud?
*note*: the following is an excerpt copied from the [RAMCloud wiki](https://ramcloud.atlassian.net/wiki/spaces/RAM/overview)  on 1/22/16.

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

The RAMCloud project is based in the Department of Computer Science at Stanford University.

# Learn More about RAMCloud
https://ramcloud.atlassian.net/wiki/spaces/RAM/overview
