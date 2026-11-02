# Apache Kafka Production Cluster Sizing & Capacity Planning Guide

**Architecture, broker sizing, topic design, partitions, storage, producer/consumer processes, high availability, and future growth**

## Table of contents

1. [Objective and sizing philosophy](#1-objective-and-sizing-philosophy)
2. [Understand the Kafka architecture](#2-understand-the-kafka-architecture-before-sizing)
3. [Collect workload information](#3-collect-workload-information)
4. [Decide the number of Kafka brokers](#4-how-to-decide-kafka-broker-count)
5. [Decide the number of topics](#5-how-to-decide-the-number-of-topics)
6. [Calculate partition count](#6-how-to-calculate-partition-count)
7. [Size consumer groups and consumer processes](#7-consumer-group-and-consumer-process-sizing)
8. [Calculate Kafka storage](#8-how-to-calculate-kafka-storage)
9. [Size broker hardware](#9-broker-hardware-sizing)
10. [Account for application growth](#10-account-for-application-growth)
11. [Production monitoring and operational thresholds](#11-production-monitoring-and-operational-thresholds)
12. [Complete end-to-end worked example: e-commerce platform](#12-complete-end-to-end-worked-example-e-commerce-platform)
13. [Final production sizing checklist](#13-final-production-sizing-checklist)
14. [Recommended implementation order](#14-recommended-order-of-implementation)
15. [Technical references](#15-technical-references)

---

## 1. Objective and sizing philosophy

The objective is to design a Kafka cluster that can handle:

- Current application traffic and expected business growth.
- Multiple independent producer applications and producer processes.
- Multiple consumer applications, consumer groups, and consumer processes.
- Peak traffic, traffic bursts, and recovery after downtime.
- Broker failures, rolling upgrades, and maintenance.
- Message retention, replay, and historical data requirements.
- Increasing throughput without redesigning the entire Kafka architecture.

**The most important principle:** Do not size a Kafka cluster based only on the number of applications, topics, or servers. Size it using throughput, message size, retention, partition-level parallelism, replication, consumer fan-out, and failure requirements.

There are three separate sizing decisions:

1. **Cluster capacity:** How many brokers, and how much CPU, RAM, network, and disk?
2. **Topic capacity:** How many topics and partitions, and what replication factor?
3. **Application capacity:** How many producer instances, consumer groups, and consumer instances?

These decisions are related, but they are not interchangeable.

## 2. Understand the Kafka architecture before sizing

A Kafka architecture consists of the following components:

| Component | What it represents | What determines its size? |
|---|---|---|
| Broker | Kafka server storing and serving data | Throughput, storage, CPU, network |
| Topic | Logical stream of events | Business domain, data contract, retention |
| Partition | Ordered log within a topic | Throughput and consumer parallelism |
| Replica | Copy of a partition | Availability and durability |
| Producer | Application writing events | Aggregate write rate, message size, batching, connections |
| Consumer group | Independent logical subscriber | Number of readers and processing requirements |
| Consumer instance | Individual process/member in a group | Partition count and processing throughput |
| Controller | Manages cluster metadata | Metadata quorum and availability |

### 2.1 Multiple producer and consumer processes

Example architecture:

```text
PRODUCERS
  Order API       (3 processes) ----\
  Payment API     (2 processes) -----+----> Kafka topic: orders.events
  Inventory API   (4 processes) ----/       36 partitions, RF=3
                                                |
                 +------------------------------+------------------------------+
                 |                              |                              |
                 v                              v                              v
        Consumer group A              Consumer group B              Consumer group C
        order-processing              analytics                     search-indexing
        12 instances                  8 instances                    6 instances
```

The producer and consumer sides have different scaling rules:

1. Nine producer processes do not require nine partitions. Producers can concurrently write to the same topic partitions.
2. Twelve consumer instances in one group need enough partitions to keep twelve instances busy.
3. A separate consumer group receives its own complete logical stream. Three independent groups can therefore generate approximately three times the logical consumer read traffic.
4. Consumer processes sharing the same `group.id` divide the workload. Processes with different group IDs receive independent copies of the stream.

A partition is assigned to at most one consumer within a particular consumer group at a time. Separate consumer groups independently consume the topic.

## 3. Collect workload information

Do not begin by selecting three, five, or ten brokers. First establish the application's workload.

| Parameter | Measurement required |
|---|---|
| Producer applications | Number of independent applications |
| Producer processes | Number of running instances across applications |
| Average event size | Bytes per message |
| Average write rate | Messages/sec and MB/sec |
| Peak write rate | Peak messages/sec and MB/sec |
| Traffic growth | Expected growth over 12–24 months |
| Consumer applications | Number of independent use cases |
| Consumer groups | Number of independent subscribers |
| Consumer processes | Instances in each consumer group |
| Consumer processing rate | Messages/sec or MB/sec per instance |
| Retention | Hours or days of required replay |
| Availability | Number of broker failures to tolerate |
| Latency | Required producer acknowledgement and consumer processing latency |

**Measure both messages/sec and MB/sec.** A system producing 20,000 messages/sec at 500 bytes has very different network and storage requirements from one producing 20,000 messages/sec at 20 KB.

### 3.1 Calculate forecast workload

Use:

```text
Forecast peak = Current observed peak × Growth factor
```

Example:

- Current peak ingestion: 40 MB/sec.
- Forecast growth: 50%.
- Future peak ingestion: 60 MB/sec.

Add a separate burst factor when the growth forecast does not already include sudden spikes.

Use forecast peak, not just today's average, for throughput and failover planning. Use average stored bytes/sec for the initial retention calculation.

## 4. How to decide Kafka broker count

Broker count is determined by four dimensions:

1. CPU, network, and disk throughput.
2. Total partition replicas and their distribution.
3. Storage capacity.
4. Availability when a broker fails or is under maintenance.

### 4.1 Initial broker recommendations

| Cluster type | Initial topology | Replication factor |
|---|---:|---:|
| Local development | 1 broker | 1 |
| Integration/testing | 3 brokers | 2–3 |
| Small production | 3 brokers minimum for RF=3 | 3 |
| Medium production | 4–6+ brokers, benchmark-dependent | 3 |
| Large production | Capacity- and failure-test-driven | 3 |

These are starting topologies, not a guarantee of production capacity.

For replication factor 3, at least three brokers are required. Spread replicas across independent availability zones or racks where possible.

A three-broker production cluster can be valid, but it has limited operational headroom. Adding a fourth or fifth broker can provide additional capacity and flexibility during maintenance.

### 4.2 Understand replication factor

| Replication factor | Meaning |
|---|---|
| 1 | One copy; no replica redundancy |
| 2 | Two copies; can tolerate one replica failure without losing all copies |
| 3 | Three copies; common production choice |

For a typical durable production topic, a useful starting configuration is:

```properties
replication.factor=3
min.insync.replicas=2
```

Producer configuration:

```properties
acks=all
```

With these settings, all replicas currently in the ISR must acknowledge an `acks=all` write, and the write cannot succeed when fewer than two in-sync replicas remain.

This provides a common balance between durability and availability. Validate the exact behavior against the Kafka version and producer semantics used in the deployment.

### 4.3 Calculate total partition replicas

Use:

```text
Total partition replicas = Σ (Topic partitions × Replication factor)
```

Example:

| Topic | Partitions | RF | Partition replicas |
|---|---:|---:|---:|
| orders.events | 36 | 3 | 108 |
| payments.events | 24 | 3 | 72 |
| inventory.events | 12 | 3 | 36 |
| notifications.events | 12 | 3 | 36 |
| **Total** | **84** | | **252** |

With six brokers:

```text
Average partition replicas per broker = 252 / 6 = 42
```

This is an average, not a placement guarantee. Check actual replica distribution and leader distribution.

Do not use the total partition count without accounting for replication.

### 4.4 Determine broker count from throughput

The best approach is to benchmark the intended production workload on the intended broker hardware.

Test the following together:

- Concurrent producer processes.
- All independent consumer groups.
- Replication factor 3.
- Compression and acknowledgement settings.
- Realistic key distribution.
- Expected partitions and message sizes.
- Peak ingestion and consumer read traffic.

Then measure how much load one broker can safely handle while meeting latency requirements.

A simplified capacity calculation is:

```text
Required brokers = max(topology minimum, throughput capacity,
                       partition capacity, storage capacity)
```

For a failure scenario that removes `k` brokers from service, a simplified capacity estimate is:

```text
N >= ceil(Required cluster workload / Safe capacity per broker) + k
```

For one broker failure, `k=1`. For a three-AZ cluster with three brokers per AZ, losing one entire AZ means `k=3`. This equation applies to a workload expressed in the same units as the benchmark. Evaluate CPU, network, disk throughput, partition replicas, and the actual failure domain separately, then take the largest result. For an AZ failure, also make sure surviving brokers have enough capacity and that replica placement leaves the required number of in-sync copies.

**Important:** There is no universal MB/sec capacity per Kafka broker. It depends on hardware, storage, compression, batching, partition count, replication, and consumer fan-out. Use a benchmark rather than assuming a fixed throughput per broker.

### 4.5 KRaft controllers

Do not confuse controller nodes with broker nodes.

For critical production environments, consider a dedicated KRaft controller quorum:

- 3 controllers: tolerates one controller failure.
- 5 controllers: tolerates two controller failures.

Controllers manage the metadata quorum; they are not a substitute for broker storage and throughput capacity. Dedicated controller and broker roles are generally preferable for critical deployments over combined broker/controller mode.

## 5. How to decide the number of topics

Topic count should be driven by logical data boundaries and operational requirements, not by the number of application instances.

Create separate topics when there is a meaningful difference in:

- Business domain or event contract.
- Retention requirements.
- Security, access permissions, or data sensitivity.
- Partition key and ordering requirements.
- Processing priority or operational SLA.
- Ownership and lifecycle.

### 5.1 Recommended topic design

| Topic | Purpose | Suggested key |
|---|---|---|
| orders.events | Order lifecycle events | `order_id` |
| payments.events | Payment lifecycle events | `payment_id` |
| inventory.events | Inventory changes | `product_id` |
| customer.events | Customer changes | `customer_id` |
| notifications.events | Notification events | `customer_id` |
| orders.dlq | Failed order messages | `order_id` |

### 5.2 Avoid these design mistakes

| Bad design | Better design |
|---|---|
| One topic per producer process | Shared topic for the same event contract |
| One topic per consumer process | Independent consumer groups |
| One topic per customer | Shared topic partitioned by customer ID |
| One topic per application instance | Logical business-domain topics |
| One topic per message type regardless of need | Group compatible event types where contracts and operational requirements allow |

For example, four producer applications can publish order lifecycle events to the same `orders.events` topic if they share the same event contract and operational requirements.

Likewise, five consumer applications can independently subscribe to it through five different consumer groups.

## 6. How to calculate partition count

Partitions determine:

1. Maximum consumer parallelism per group.
2. Parallel write capacity and data distribution.
3. Distribution of leaders and replicas across brokers.
4. Ordering boundaries.
5. Resource overhead across the cluster.

A partition is an ordered log. Kafka preserves ordering within a partition, not across all partitions of a topic.

### 6.1 Partition count based on consumer instances

Suppose the maximum planned consumer concurrency is:

| Consumer group | Current instances | Expected future instances |
|---|---:|---:|
| order-processing | 6 | 12 |
| analytics | 4 | 16 |
| search-indexing | 3 | 8 |
| audit-processing | 2 | 4 |

If all groups subscribe to the same topic, the maximum desired concurrency in any one group is 16.

Therefore:

```text
Minimum partitions for concurrency = 16
```

This does not mean 16 partitions necessarily provide sufficient throughput. Throughput must also be considered.

### 6.2 Partition count based on throughput

Use this practical formula:

\[
P = \max\left(
\left\lceil\frac{T_w}{W_p}\right\rceil,
\max_g\left\lceil\frac{T_{r,g}}{R_{p,g}}\right\rceil,
C_{\max}
\right)
\]

Where:

- \(P\) = required partitions.
- \(T_w\) = peak topic write throughput in MB/sec.
- \(W_p\) = measured sustainable write throughput per partition.
- \(T_{r,g}\) = peak consumption throughput for group \(g\).
- \(R_{p,g}\) = measured sustainable processing throughput per partition for group \(g\).
- \(C_{\max}\) = maximum desired simultaneous consumer instances in any group.

This is a starting calculation. Benchmark results must account for partition skew, producer batching, replication, acknowledgements, and the actual consumer application.

### 6.3 Include future growth

Do not choose partitions based only on current concurrency.

A simple approach is:

```text
Final partitions = Calculated partitions × (1 + headroom percentage)
```

For example, use 20% headroom where that matches expected growth and the partition-overhead budget. Round to a practical whole number and validate key distribution.

### 6.4 Worked partition example

Assume:

| Input | Value |
|---|---:|
| Current peak ingestion | 40 MB/sec |
| Growth forecast | 50% |
| Forecast peak ingestion | 60 MB/sec |
| Measured write capacity per partition | 5 MB/sec |
| Slowest consumer processing capacity per partition | 2 MB/sec |
| Maximum planned consumers in a group | 24 |
| Additional partition headroom | 20% |

**Step 1: Write throughput**

```text
60 / 5 = 12 partitions
```

**Step 2: Consumer throughput**

```text
60 / 2 = 30 partitions
```

**Step 3: Consumer concurrency**

```text
24 partitions
```

**Step 4: Select the largest requirement**

```text
max(12, 30, 24) = 30
```

**Step 5: Add headroom**

```text
30 × 1.20 = 36
```

**Illustrative result: 36 partitions.** The 2 MB/sec and 5 MB/sec values must come from a realistic performance test, not assumed Kafka defaults.

### 6.5 Partition count and ordering

Suppose orders are keyed by `order_id`.

```text
key = order_id
```

This keeps events for the same order on the same partition under normal keyed partitioning.

Avoid random keys when event ordering for a business entity is important.

Be careful when increasing partitions on an existing keyed topic. The default key-to-partition mapping may change, potentially sending future records for an existing key to a different partition. That can complicate ordering and stateful consumer processing.

Before production:

- Choose a realistic initial partition count.
- Avoid unnecessary partition inflation.
- Design for business-key distribution.
- Test hot keys.
- Have a migration strategy if repartitioning is needed.

### 6.6 How many partitions per broker?

There is no universal hard limit that fits all workloads. Published operational guidance sometimes uses approximately 100–200 partitions per broker as an initial planning baseline, but this is not an Apache Kafka guarantee or a sizing substitute. Actual limits depend on broker hardware, Kafka version, storage, replication, and workload.

Always calculate:

```text
Total partition replicas / Number of brokers
```

Then test broker CPU, memory, file descriptors, replication recovery, and controller performance.

## 7. Consumer group and consumer process sizing

This deserves special attention for applications running multiple processes, containers, or Kubernetes pods.

### 7.1 Understand the difference

| Scenario | What happens? |
|---|---|
| 3 processes, same group ID | Processes share messages |
| 3 processes, different group IDs | Each group receives the full stream |
| 10 processes, 5 partitions, same group | At most 5 processes can be assigned partitions |
| 10 processes, 30 partitions, same group | Up to 10 processes can receive partitions |

Example: a topic has 12 partitions and one consumer group has three members. The partitions can be distributed approximately four per member. If the group scales to 15 members, at most 12 members can receive a partition; at least three will be idle for that topic.

### 7.2 Multiple consumer applications

Suppose three applications consume the same topic:

| Consumer application | Group ID | Instances | Purpose |
|---|---|---:|---|
| Order processor | `order-service` | 12 | Execute business logic |
| Reporting service | `reporting-service` | 8 | Build reports |
| Search service | `search-service` | 6 | Update search index |

All three groups receive the topic independently.

Kafka therefore needs to handle:

- One producer write stream.
- Three independent consumer read streams.
- Replication traffic.
- Additional traffic when consumers replay historical data.

### 7.3 Calculate total consumer instances

For every consumer group, calculate:

```text
Total instances = Application replicas × Consumers per application replica
```

Example:

| Application | Kubernetes pods | Consumer concurrency per pod | Total group members |
|---|---:|---:|---:|
| Order processing | 4 | 3 | 12 |
| Analytics | 4 | 2 | 8 |
| Search indexing | 3 | 2 | 6 |
| Audit processing | 2 | 2 | 4 |
| **Total** | | | **30** |

The total of 30 consumer members does not mean 30 partitions are automatically required. Each group has its own assignment and parallelism requirement.

### 7.4 Process management recommendation

Use a stable, logical `group.id` for each independently running application.

For example:

- `order-service-prod`
- `analytics-service-prod`
- `search-indexer-prod`

Do not append a unique process ID, container ID, or pod ID to the group ID for consumers that are supposed to share work. Doing so would create independent groups and make each process read the complete stream.

### 7.5 Consumer scaling considerations

Before increasing consumers, investigate whether the bottleneck is:

- Insufficient partitions.
- Slow business logic.
- Database or API response time.
- Large message deserialization overhead.
- Too much work per poll batch.
- Consumer rebalance frequency.
- Downstream rate limiting.

Adding more consumers will not automatically improve performance if the downstream database is already saturated.

## 8. How to calculate Kafka storage

Storage is determined by:

- Incoming data volume.
- Message retention.
- Compression and actual bytes stored.
- Replication factor.
- Log segment and index overhead.
- Safety margin for growth and recovery.

### 8.1 Storage formula

For topics using delete-based retention:

\[
S_{primary} = R_{stored} \times T_{retention}
\]

\[
S_{cluster} = S_{primary} \times RF
\]

\[
S_{provisioned} = \frac{S_{cluster} \times F_{safety}}{U_{target}}
\]

Where:

- \(R_{stored}\) = average compressed bytes/sec actually stored.
- \(T_{retention}\) = retention period in seconds.
- \(RF\) = replication factor.
- \(F_{safety}\) = additional allowance for overhead and growth.
- \(U_{target}\) = maximum planned disk utilization, expressed as a decimal (for example, 0.70 for 70%).

Calculate storage separately for each topic when retention or compression differs, then add the results.

The formula above gives total cluster capacity when all brokers are available. For a failure scenario that removes `k` brokers (N-k), account for the smaller number of surviving disks. With `N` equal-sized brokers:

\[
S_{provisioned,N-k} = \frac{S_{primary} \times RF \times F_{safety}}{U_{target} \times ((N-k)/N)}
\]

Equivalently, the minimum usable disk per broker is:

```text
Per-broker usable disk >= (Primary retained data × RF × Safety factor)
                         / (Target utilization × (N - k))
```

Use `k=1` for one broker loss. If an availability zone contains three brokers, use `k=3` when sizing for loss of that entire zone. This assumes the surviving brokers must have enough capacity to restore the intended replica count after the failure. Include filesystem, operating-system, and reserved-volume space when translating usable Kafka data capacity into provisioned disk.

### 8.2 Worked storage example

Assume:

| Parameter | Value |
|---|---:|
| Average compressed disk ingestion | 10 MB/sec |
| Retention | 7 days |
| Replication factor | 3 |
| Safety factor | 1.20 |
| Maximum planned disk utilization | 70% |

**Step 1: Calculate seven days of primary storage**

```text
10 MB/sec × 86,400 sec/day × 7 days
= 6,048,000 MB
≈ 6.05 TB (decimal units)
```

**Step 2: Include replication**

```text
6.048 TB × 3 = 18.144 TB
```

**Step 3: Include safety margin and target utilization**

```text
(18.144 TB × 1.20) / 0.70 = 31.1 TB
```

**Illustrative result: approximately 31.1 TB of provisioned cluster storage.**

If using six equally sized brokers:

```text
31.1 TB / 6 ≈ 5.18 TB per broker
```

Provision approximately 5.2 TB per broker as a starting point, then round up for actual filesystem, volume, and workload requirements.

This assumes delete-based retention, uniform distribution, and no additional storage features that materially change the calculation. It is an initial capacity estimate, not a replacement for a disk benchmark.

### 8.3 Storage sizing by topic

Do not apply seven days of retention to all topics automatically.

| Topic | Average stored rate | Retention | RF |
|---|---:|---:|---:|
| orders.events | 10 MB/sec | 7 days | 3 |
| payments.events | 3 MB/sec | 30 days | 3 |
| inventory.events | 2 MB/sec | 3 days | 3 |
| notifications.events | 1 MB/sec | 2 days | 3 |

The payment topic can consume more storage than its relatively low throughput suggests because of the longer retention.

### 8.4 Do consumer groups increase storage?

No, not directly.

| Activity | Storage impact | Throughput impact |
|---|---|---|
| Add producer process | No additional retained copy | More aggregate input if it produces more |
| Add consumer instance in same group | No additional retained copy | Changes read parallelism |
| Add independent consumer group | No additional retained copy | Additional read traffic |
| Increase replication factor | More retained copies | More replication traffic |
| Increase retention | More retained data | More disk and possibly background I/O |
| Replay old data | Normally no additional retained copy | Extra read traffic |

This is an important distinction when sizing applications with many consumers.

### 8.5 Retention policy

Use retention based on business recovery and replay needs.

For example:

- If consumers may remain unavailable for 24 hours, retention should cover that outage.
- Add additional time for recovery and historical replay.
- Consider legal, audit, and business retention requirements separately.
- Monitor consumer lag so consumers do not fall behind the retention window.

Kafka's `retention.ms` controls time-based retention for delete-policy logs. `retention.bytes` is enforced per partition, so an approximate topic-level byte limit is:

```text
Approximate topic limit = Partition count × retention.bytes
```

Actual deletion is segment-based, so precise physical usage can vary. For compacted topics, use a separate capacity model. Compaction preserves the latest record per key rather than simply deleting records after an ordinary retention window.

## 9. Broker hardware sizing

After calculating the number of brokers, determine the resources per broker.

Avoid choosing CPU and RAM using fixed rules such as 4 CPUs and 16 GB RAM for every Kafka broker. Workload and storage technology matter.

| Resource | What to consider | Main concern |
|---|---|---|
| CPU | Serialization, compression, request handling, replication | Processing throughput |
| RAM | JVM heap, metadata, and OS page cache | Cache effectiveness and GC |
| Disk | SSD/NVMe, sustained throughput, latency, and capacity | Log writes and reads |
| Network | Incoming, outgoing, and replication traffic | Network saturation |
| File descriptors | Number of partitions, log files, and connections | Resource exhaustion |
| JVM | Heap, garbage collection, and version-specific settings | Latency spikes |

### 9.1 CPU

Increase CPU capacity when broker CPU remains high during peak periods, or when compression, TLS, request processing, and replication are CPU-bound.

### 9.2 RAM

Do not assume all available RAM should be assigned to the JVM heap.

Operating-system page cache is important for Kafka log reads. Reserve memory for the operating system and filesystem caching, in addition to JVM and broker requirements.

### 9.3 Disk

Prefer storage that has been validated for sustained Kafka workloads.

Evaluate:

- Sequential write throughput.
- Sequential read throughput.
- Disk latency.
- IOPS under concurrent producer and consumer loads.
- Performance under replication and recovery.
- Free storage during retention and segment cleanup.

### 9.4 Network

Account for all of the following:

1. Producer-to-broker traffic.
2. Broker-to-broker replication traffic.
3. Broker-to-consumer traffic.
4. Inter-broker traffic during replica reassignment.
5. Consumer replay and recovery traffic.

For example, if three independent consumer groups each read a topic's entire 60 MB/sec stream, logical consumer read throughput is approximately 180 MB/sec, excluding protocol overhead and other traffic.

### 9.5 Producer optimization

Producer configuration can significantly change broker utilization.

| Configuration | Purpose |
|---|---|
| `compression.type` | Reduce network and retained bytes |
| `batch.size` | Improve write batching |
| `linger.ms` | Allow additional records to join batches |
| `acks` | Control write acknowledgement requirements |
| `enable.idempotence` | Protect against duplicate records from supported producer retries |

Batching is particularly useful when multiple records are sent to the same partition. Compression effectiveness also depends on batch formation.

Do not compensate for inadequate partitioning or broker resources merely by increasing producer batching or buffer sizes.

## 10. Account for application growth

A production-grade architecture should have a growth plan rather than just sufficient capacity for current traffic.

Consider three growth dimensions independently.

### 10.1 Business traffic growth

Example:

| Period | Expected peak ingestion |
|---|---:|
| Today | 40 MB/sec |
| 6 months | 50 MB/sec |
| 12 months | 60 MB/sec |
| 24 months | 90 MB/sec |

Use the forecast that matches the expected operating life of your infrastructure decisions.

### 10.2 Application process growth

Example:

| Component | Today | Expected future |
|---|---:|---:|
| Producer services | 4 | 8 |
| Producer instances | 12 | 24 |
| Consumer groups | 3 | 6 |
| Consumer instances | 20 | 48 |

More processes do not necessarily require more partitions. However, more independent consumer groups can increase Kafka's read traffic significantly.

### 10.3 Retention growth

Example:

| Business requirement | Today | Future |
|---|---:|---:|
| Orders retention | 7 days | 14 days |
| Payments retention | 30 days | 90 days |
| Audit retention | 30 days | 180 days |

Retention growth can make storage the biggest future constraint even if application throughput does not change.

### 10.4 Scaling strategy

| Bottleneck | Appropriate response |
|---|---|
| Overall broker CPU saturation | Increase broker capacity |
| Broker disk capacity | Expand storage or add brokers and redistribute replicas |
| Broker network saturation | Upgrade network or distribute workload across more capacity |
| Too few partitions | Increase partitions with an ordering/migration plan |
| Consumer processing lag | Improve application processing or increase group parallelism |
| Too many consumer groups | Review application subscriptions and data fan-out requirements |
| Hot key | Review the partitioning strategy and business key distribution |
| Excessive replay traffic | Plan recovery capacity and consumer replay windows |

**Remember:** Adding brokers alone does not necessarily make a topic faster. If a topic has only one partition, that partition cannot use all the brokers concurrently for its normal leader-side processing.

## 11. Production monitoring and operational thresholds

Sizing is incomplete unless the cluster can be monitored and scaled before problems become outages.

| Area | Metrics to monitor | Warning signs |
|---|---|---|
| Broker | CPU, memory, network, disk | Sustained resource saturation |
| Storage | Disk utilization, I/O latency | Low free space, rising latency |
| Replication | Under-replicated partitions, ISR | Replication falling behind |
| Broker availability | Offline partitions, controller health | Missing leaders or quorum issues |
| Producer | Request latency, retries, errors | Increased delivery failures |
| Consumer | Lag, records consumed/sec | Lag growing continuously |
| Consumer group | Rebalances, members, assignment | Frequent reassignment |
| Throughput | Bytes in/sec and bytes out/sec | Approaching tested capacity |
| Recovery | Replica catch-up rate, lag recovery | Recovery slower than new incoming traffic |

Useful operational policies include:

- Alert before a disk becomes nearly full.
- Alert when under-replicated partitions persist.
- Alert when consumer lag exceeds its business recovery objective.
- Monitor producer request latency and retries.
- Check broker performance during peak load, not only averages.
- Regularly test broker failure and recovery.

A particularly important production test is whether consumers can catch up faster than new messages arrive after a failure.

For example, if a consumer normally processes 60 MB/sec while producers generate 60 MB/sec, it cannot reduce an existing backlog while maintaining only 60 MB/sec of consumption. Recovery capacity must be higher than ongoing ingestion.

## 12. Complete end-to-end worked example: e-commerce platform

This section brings topic design, producer processes, consumer groups, partitions, replication, broker count, storage, network, security, monitoring, failure handling, and future growth into one consistent example. The numbers are illustrative. In a real design, replace benchmark assumptions with measurements from the exact Kafka version, instance type, disk, network, compression, record-size distribution, and client configuration you will deploy.

### 12.1 Business scenario and assumptions

An e-commerce platform has six producer applications. They publish order, payment, inventory, customer, and notification events. Several independent applications consume those events for business processing, analytics, fraud detection, search, and reporting.

The platform wants to:

- Support the forecast peak workload for the next 12 months.
- Tolerate one broker failure and, for this example's production target, continue operating through the loss of one entire availability zone.
- Preserve acknowledged data subject to the configured ISR and producer acknowledgement policy.
- Run across three availability zones with replica placement spread across zones.
- Retain each event type for the period required by its business use case.
- Scale consumers independently without accidentally creating duplicate full-stream subscriptions.
- Keep enough storage and throughput headroom to recover after a broker or consumer outage.

**Sizing convention for this example:** The peak-throughput numbers below are normalized application-payload MB/sec from a representative load test. Use the same unit consistently for producer, partition-write, and consumer-processing comparisons. Storage rates are separately measured compressed bytes actually written to Kafka logs. Do not mix uncompressed payload MB/sec with compressed disk MB/sec in a real worksheet.

### 12.2 Forecast traffic and event catalog

The platform has the following forecast peak rates and measured average compressed storage rates. The 12-month forecast already includes the expected business growth from today's baseline. It is not an average; it is the peak planning rate. For the approximate record-rate column below, MB and KB use decimal units (1 MB = 1,000,000 bytes; 1 KB = 1,000 bytes), and the average event size includes the serialized key/value and representative headers.

| Topic | Event purpose | Forecast peak payload rate | Approx. peak records/sec | Average compressed disk rate | Average event size | Retention |
|---|---|---:|---:|---:|---:|---:|
| `orders.events` | Order created, confirmed, shipped, cancelled | 18 MB/sec | 12,000 | 3.00 MB/sec | 1.5 KB | 7 days |
| `payments.events` | Payment authorized, captured, refunded, failed | 8 MB/sec | 8,000 | 1.00 MB/sec | 1.0 KB | 30 days |
| `inventory.events` | Stock and availability changes | 12 MB/sec | 15,000 | 1.50 MB/sec | 0.8 KB | 3 days |
| `customer.events` | Customer profile changes | 4 MB/sec | 3,333 | 0.50 MB/sec | 1.2 KB | 14 days |
| `notifications.events` | Notification requested and delivery status | 10 MB/sec | 14,286 | 1.00 MB/sec | 0.7 KB | 2 days |
| `orders.dlq` | Permanently failed order events | 0.2 MB/sec | Varies | 0.05 MB/sec | Varies | 14 days |
| `payments.dlq` | Permanently failed payment events | 0.1 MB/sec | Varies | 0.02 MB/sec | Varies | 30 days |
| **Total** | | **52.3 MB/sec** | **~52,619 business records/sec** | **7.07 MB/sec** | | |

The five business topics account for 52 MB/sec of forecast peak ingestion. The two DLQ rates are deliberately small in normal operation; DLQ replay or a mass failure can create short-lived bursts, so recovery throughput must be tested separately.

The average compressed storage rate is not derived from the peak rate. It is measured from representative event data after producer compression. If compression or event composition changes, repeat the storage measurement.

### 12.3 Producer applications and processes

Assume each listed running pod/process has one long-lived Kafka producer client. Kafka producer clients are thread-safe in common Kafka client implementations, so do not create a new producer for every HTTP request or message. The actual producer-client count depends on application architecture, not on the topic partition count.

| Producer application | Running processes/pods | Topics published | Example responsibility |
|---|---:|---|---|
| Order API | 6 | `orders.events` | Publish order lifecycle changes |
| Payment gateway | 3 | `payments.events` | Publish payment lifecycle changes |
| Inventory service | 4 | `inventory.events` | Publish stock changes |
| Customer service | 2 | `customer.events` | Publish customer profile changes |
| Notification service | 3 | `notifications.events` | Publish notification events |
| Batch import service | 2 | `orders.events`, `customer.events` | Publish bulk imports |
| **Total** | **20 producer processes/clients** | | |

The producer processes for the same event contract can write to the same topic. They do not need one partition each. The producer's key determines the partition for keyed records; producer concurrency is handled by the Kafka client and broker request paths.

Use business keys to preserve per-entity ordering:

| Topic | Suggested record key | Ordering scope |
|---|---|---|
| `orders.events` | `order_id` | Events for one order |
| `payments.events` | `payment_id` | Events for one payment |
| `inventory.events` | `product_id` or `warehouse_id + product_id` | Changes for one inventory entity |
| `customer.events` | `customer_id` | Changes for one customer |
| `notifications.events` | `notification_id` or `customer_id` | Depends on required ordering |
| DLQ topics | Preserve original key | Preserve the original entity grouping where possible |

Check key skew before launch. A very large customer, product, or tenant can become a hot key and overload one partition even when total cluster capacity looks sufficient.

### 12.4 Consumer groups and independent processes

Each independently interested application gets its own stable consumer group ID. Processes within one group share the partitions. Different groups each receive the full logical topic stream.

| Topic | Consumer group ID | Pods | Consumers per pod | Group members | Purpose |
|---|---|---:|---:|---:|---|
| `orders.events` | `order-fulfillment-prod` | 6 | 2 | 12 | Execute order workflow |
| `orders.events` | `order-analytics-prod` | 4 | 2 | 8 | Analytics pipeline |
| `orders.events` | `fraud-detection-prod` | 3 | 2 | 6 | Fraud checks |
| `orders.events` | `order-audit-prod` | 3 | 1 | 3 | Audit/archive |
| `payments.events` | `payment-settlement-prod` | 4 | 2 | 8 | Settlement workflow |
| `payments.events` | `payment-fraud-prod` | 2 | 2 | 4 | Payment fraud checks |
| `payments.events` | `payment-analytics-prod` | 2 | 2 | 4 | Payment analytics |
| `payments.events` | `payment-recon-prod` | 2 | 1 | 2 | Reconciliation |
| `inventory.events` | `inventory-projection-prod` | 3 | 2 | 6 | Build inventory projection |
| `inventory.events` | `availability-cache-prod` | 2 | 2 | 4 | Update availability cache |
| `inventory.events` | `inventory-analytics-prod` | 2 | 2 | 4 | Inventory analytics |
| `customer.events` | `crm-sync-prod` | 2 | 2 | 4 | Synchronize CRM |
| `customer.events` | `personalization-prod` | 3 | 1 | 3 | Update personalization data |
| `customer.events` | `customer-analytics-prod` | 3 | 1 | 3 | Customer analytics |
| `notifications.events` | `notification-dispatch-prod` | 4 | 2 | 8 | Send notifications |
| `notifications.events` | `delivery-analytics-prod` | 2 | 1 | 2 | Delivery analytics |
| `orders.dlq` | `orders-dlq-recovery-prod` | 2 | 2 | 4 | Investigate/reprocess failed orders |
| `payments.dlq` | `payments-dlq-recovery-prod` | 2 | 2 | 4 | Investigate/reprocess failed payments |

There are 18 independent consumer groups and 89 consumer process members in total. **Do not use 89 as the partition requirement.** Partition concurrency is evaluated separately for each topic and each group. For example, `orders.events` needs to support the largest group (12 members), not the sum of all four groups (29 members), while the broker must serve all four groups' read traffic.

If a pod runs two consumer instances in the same group, those are two group members. If the application instead uses one consumer with worker threads, measure that design directly: worker-thread count is not automatically equal to Kafka consumer count or partition concurrency.

### 12.5 Decide topic and partition counts

The seven topics above are separated by business contract, retention, ownership, and processing requirements. The DLQs are separate so that failed records can be inspected and replayed without mixing them into normal event traffic. For a larger design, also consider retry topics with controlled backoff rather than repeatedly retrying poison messages in-place.

For each topic, calculate three requirements:

1. Write throughput: forecast topic peak rate divided by tested sustainable write rate per partition.
2. Consumer throughput: topic peak rate divided by the slowest critical consumer group's tested processing rate per partition.
3. Consumer concurrency: maximum number of desired simultaneous consumers in any one group for that topic.

Then take the largest result and add headroom. The per-partition capacities in this example are assumed to come from load tests using the actual message mix and key distribution.

| Topic | Peak write rate | Tested write capacity per partition | Slowest critical group capacity per partition | Largest group size | Write partitions | Read partitions | Base partitions | +25% headroom | Selected partitions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `orders.events` | 18 MB/s | 3 MB/s | 1 MB/s | 12 | 6 | 18 | 18 | 22.5 | **24** |
| `payments.events` | 8 MB/s | 2 MB/s | 1 MB/s | 8 | 4 | 8 | 8 | 10 | **12** |
| `inventory.events` | 12 MB/s | 3 MB/s | 1.5 MB/s | 6 | 4 | 8 | 8 | 10 | **12** |
| `customer.events` | 4 MB/s | 2 MB/s | 1 MB/s | 4 | 2 | 4 | 4 | 5 | **6** |
| `notifications.events` | 10 MB/s | 2 MB/s | 1.25 MB/s | 8 | 5 | 8 | 8 | 10 | **12** |
| `orders.dlq` | 0.2 MB/s | 2 MB/s | 1 MB/s | 4 | 1 | 1 | 4 | 5 | **6** |
| `payments.dlq` | 0.1 MB/s | 1 MB/s | 0.5 MB/s | 4 | 1 | 1 | 4 | 5 | **6** |
| **Total** | **52.3 MB/s** | | | | | | | | **78** |

For `orders.events`:

```text
Write requirement = ceil(18 / 3) = 6 partitions
Read requirement  = ceil(18 / 1) = 18 partitions
Concurrency       = 12 partitions
Base requirement  = max(6, 18, 12) = 18 partitions
With 25% headroom = 18 × 1.25 = 22.5
Selected           = 24 partitions
```

The same calculation is applied to every topic. The selected count is rounded to a practical whole number; it is not a Kafka requirement that partition count be a multiple of broker count.

**Result:** 78 topic partitions in total. With replication factor 3:

```text
Partition replicas = 78 × 3 = 234 replicas
```

With six brokers, the arithmetic average is:

```text
234 / 6 = 39 partition replicas per broker
```

The actual replica and leader assignment must be checked. An even average does not prove that leaders, disk usage, or hot partitions are balanced.

### 12.6 Consumer concurrency check

Now compare the selected partitions with the planned group members:

| Topic | Selected partitions | Largest group | Maximum active members for that topic | Members potentially idle at planned peak |
|---|---:|---:|---:|---:|
| `orders.events` | 24 | 12 | 12 | 0 |
| `payments.events` | 12 | 8 | 8 | 0 |
| `inventory.events` | 12 | 6 | 6 | 0 |
| `customer.events` | 6 | 4 | 4 | 0 |
| `notifications.events` | 12 | 8 | 8 | 0 |
| `orders.dlq` | 6 | 4 | 4 | 0 |
| `payments.dlq` | 6 | 4 | 4 | 0 |

There are extra partitions beyond the largest group size to support throughput and growth. They do not mean every group must run one consumer per partition. Some members can own multiple partitions.

If a group grows beyond the topic's partition count, additional members in that group will be idle for that topic. If a group is subscribed to multiple topics, assignment behavior depends on the subscription and assignor; evaluate the actual topic-partition assignment rather than simply adding all topic partition counts or process counts.

### 12.7 Storage calculation across all topics

Use the measured average compressed disk rate and each topic's retention. Decimal TB is used here: 1 TB = 1,000,000 MB.

```text
Primary retained TB = compressed disk MB/sec × retention seconds / 1,000,000
```

| Topic | Compressed disk rate | Retention | Primary retained data |
|---|---:|---:|---:|
| `orders.events` | 3.00 MB/s | 7 days | 1.8144 TB |
| `payments.events` | 1.00 MB/s | 30 days | 2.5920 TB |
| `inventory.events` | 1.50 MB/s | 3 days | 0.3888 TB |
| `customer.events` | 0.50 MB/s | 14 days | 0.6048 TB |
| `notifications.events` | 1.00 MB/s | 2 days | 0.1728 TB |
| `orders.dlq` | 0.05 MB/s | 14 days | 0.0605 TB |
| `payments.dlq` | 0.02 MB/s | 30 days | 0.0518 TB |
| **Total** | **7.07 MB/s** | | **5.6851 TB** |

Replication factor 3:

```text
Replica storage = 5.6851 TB × 3 = 17.0554 TB
```

Add 30% for segment/index variation, unevenness, expected near-term growth, and operational allowance:

```text
Planning data budget = 17.0554 TB × 1.30 = 22.1720 TB
```

For the selected topology, use nine brokers split evenly across three availability zones (three brokers per zone). If one entire zone is unavailable, six brokers remain and provide 42 TB of usable Kafka data volume:

```text
N-3 usable capacity = (9 - 3) × 7 TB = 42 TB
Planning utilization after one zone loss = 22.172 / 42 = 52.8%
```

This is below the 70% target. A six-broker cluster with two brokers per zone would leave four brokers (28 TB) after losing one zone; the planning budget would then be 22.172 / 28 = 79.2%, above the target. Therefore, nine brokers are selected for the stated one-AZ-loss storage objective. Add filesystem/system reservations when translating usable Kafka volume into cloud or physical disk sizes.

This calculation assumes delete-based retention and an average compressed disk rate representative of the whole retention window. Compacted topics require a separate model because compaction retains the latest value per key and may also have tombstone and cleanup overhead.

### 12.8 Calculate the broker count from benchmarked failure capacity

Assume a benchmark using the intended broker instance type, disk, Kafka version, RF=3, producer compression, message sizes, and consumer fan-out establishes these conservative per-broker safe limits while meeting the application's latency objectives:

- 15 MB/sec of normalized producer ingress/leader-write workload per broker.
- 60 MB/sec of normalized consumer egress workload per broker.
- 7 TB usable Kafka data volume per broker.

These are **example benchmark outputs**, not universal Kafka broker specifications.

The design evaluates both a single broker failure (`k=1`) and a complete AZ failure (`k=3`, because this topology has three brokers in each AZ). Use:

```text
Required brokers = ceil(cluster workload / safe per-broker capacity) + k
```

**Producer ingress:**

```text
One broker failure: ceil(52.3 / 15) + 1 = 4 + 1 = 5 brokers
One AZ failure:     ceil(52.3 / 15) + 3 = 4 + 3 = 7 brokers
```

**Consumer egress:** Each independent group reads the topic's stream. If all groups read the forecast topic peak simultaneously:

```text
orders.events        = 4 groups × 18 MB/s = 72 MB/s
payments.events      = 4 groups ×  8 MB/s = 32 MB/s
inventory.events     = 3 groups × 12 MB/s = 36 MB/s
customer.events      = 3 groups ×  4 MB/s = 12 MB/s
notifications.events = 2 groups × 10 MB/s = 20 MB/s
orders.dlq           = 1 group  × 0.2 MB/s = 0.2 MB/s
payments.dlq         = 1 group  × 0.1 MB/s = 0.1 MB/s
                                             ---------
Total logical reads                          172.3 MB/s
```

Then:

```text
One broker failure: ceil(172.3 / 60) + 1 = 3 + 1 = 4 brokers
One AZ failure:     ceil(172.3 / 60) + 3 = 3 + 3 = 6 brokers
```

**Storage:** To preserve the 70% utilization target and 30% planning allowance after a failure, the number of surviving brokers must be:

```text
Surviving brokers required = ceil(22.172 / (0.70 × 7)) = 5
One broker failure: total brokers = 5 + 1 = 6
One AZ failure:     total brokers = 5 + 3 = 8
```

The one-AZ storage minimum is eight brokers, but eight cannot be split evenly across three AZs. The next evenly distributed topology is nine brokers (three per AZ). With nine brokers, an AZ failure leaves six brokers: 6 × 15 = 90 MB/sec safe ingress capacity, 6 × 60 = 360 MB/sec safe consumer egress capacity, and 42 TB usable storage. These exceed the example's forecast workload and storage planning budget.

**Decision:**

| Constraint | One broker unavailable | One AZ unavailable |
|---|---:|---:|
| RF=3 topology minimum | 3 | 3 |
| Producer ingress capacity | 5 | 7 |
| Consumer egress capacity | 4 | 6 |
| Storage capacity | 6 | 8 |
| Three-AZ balanced topology | — | 9 |
| **Selected broker count** | | **9** |

Storage is the limiting dimension for the AZ-loss scenario; even distribution across three AZs rounds the selected topology to nine brokers. This is more than the six brokers needed for the stated single-broker-failure target. Validate real replica placement, leader balance, disk throughput, CPU, network, and recovery behavior in a cluster-level test.

The total of 172.3 MB/sec is a logical consumer-read workload. Actual broker network bytes depend on the compressed record-batch size, protocol overhead, fetch behavior, and replay. The benchmark must use the same compression and event mix; do not blindly compare decoded application payload bytes to NIC throughput counters.

### 12.9 Network and replication traffic check

At RF=3, each record is stored on three brokers. At the 52.3 MB/sec normalized peak write rate, the cluster performs approximately three copies of the log write workload:

```text
Aggregate replica-write workload ≈ 52.3 × 3 = 156.9 MB/sec
Additional follower-copy payload ≈ 52.3 × (3 - 1) = 104.6 MB/sec
```

These are logical workload estimates, not a complete network bill. Actual network counters depend on compression, replica fetch implementation, acknowledgements, retries, cross-zone paths, and whether the leader and follower traffic traverse the same links. Add the independent consumer fetch workload, controller/metadata traffic, reassignment, and recovery traffic to the benchmark plan.

Place replicas across three availability zones (or racks) using rack awareness, with one replica of a partition in each zone where the topology supports it. This reduces the chance that a single zone failure removes all replicas of a partition. Cross-zone replication has cost and latency implications; include those in the infrastructure design.

### 12.10 Proposed physical topology

```text
                 Application layer
    20 producer processes / 18 consumer groups
                         |
                 TLS + authentication
                         |
        +----------------+----------------+
        |                |                |
      AZ-A             AZ-B             AZ-C
   +---------+      +---------+      +---------+
   | Broker 1|      | Broker 4|      | Broker 7|
   | Broker 2|      | Broker 5|      | Broker 8|
   | Broker 3|      | Broker 6|      | Broker 9|
   +---------+      +---------+      +---------+
        \                |                /
         \_______________|_______________/
           RF=3, rack-aware replica placement

     Dedicated KRaft controllers: 3 nodes
       Controller 1 / 2 / 3 across AZs
```

Recommended starting topology for this example:

| Component | Count | Notes |
|---|---:|---|
| Kafka brokers | **9** | Three per AZ across three AZs; 7 TB usable Kafka volume per broker in this example |
| KRaft controllers | **3 dedicated** | One per AZ; maintain an odd-sized metadata quorum |
| Replication factor | **3** | Place replicas across three racks/AZs where possible |
| Minimum ISR | **2** | With producer `acks=all`, reject writes if fewer than two replicas are in sync |
| Partitions | **78** | Across seven topics |
| Partition replicas | **234** | 78 × RF3; average 26 per broker |

The controller nodes are separate from the nine brokers in this topology. Size controller CPU, memory, and disk according to metadata scale and Kafka's version-specific operational guidance; do not count controller storage as topic data storage. With three controllers across three AZs, a single AZ loss leaves two of the three controller voters, which is a majority.

### 12.11 Topic configuration examples

The following are illustrative topic settings for delete-based retention. Configure RF and minimum ISR consistently with the cluster's durability objectives.

| Topic | Partitions | RF | `min.insync.replicas` | `cleanup.policy` | `retention.ms` |
|---|---:|---:|---:|---|---:|
| `orders.events` | 24 | 3 | 2 | `delete` | `604800000` |
| `payments.events` | 12 | 3 | 2 | `delete` | `2592000000` |
| `inventory.events` | 12 | 3 | 2 | `delete` | `259200000` |
| `customer.events` | 6 | 3 | 2 | `delete` | `1209600000` |
| `notifications.events` | 12 | 3 | 2 | `delete` | `172800000` |
| `orders.dlq` | 6 | 3 | 2 | `delete` | `1209600000` |
| `payments.dlq` | 6 | 3 | 2 | `delete` | `2592000000` |

For example, create the order topic with:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-bootstrap.example.com:9092 \
  --create \
  --topic orders.events \
  --partitions 24 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000
```

Confirm topic-level settings, broker defaults, and command syntax against the deployed Kafka version. If a topic requires compaction (for example, a latest-state-per-key topic), design its cleanup policy and storage estimate separately rather than copying the delete-only settings above.

### 12.12 Producer and consumer configuration approach

Example producer settings to validate in load tests:

```properties
acks=all
enable.idempotence=true
compression.type=lz4
linger.ms=10
batch.size=65536
```

The compression codec, linger, and batch size are starting test values, not universal production values. Benchmark compression ratio, producer CPU, end-to-end latency, batch size, and broker network/disk load. Use one long-lived producer client per application process unless the client/library architecture has a measured reason to do otherwise.

Example consumer settings to evaluate:

```properties
group.id=order-fulfillment-prod
enable.auto.commit=false
max.poll.records=500
```

Commit offsets only after the corresponding work is safely completed when using an at-least-once processing design. Make side effects idempotent (for example, use an event ID or business idempotency key in a database). Tune `max.poll.records`, processing concurrency, and `max.poll.interval.ms` to the real processing time. Choose `auto.offset.reset` deliberately for new groups and recovery; `earliest` and `latest` have different replay implications and do not override offsets already committed by an existing group.

Kafka producer idempotence protects against certain duplicate writes caused by retries within the producer protocol. It does not automatically make an external database update or HTTP call exactly-once. For consume-transform-produce workflows entirely within Kafka, evaluate Kafka transactions; for external systems, use idempotent writes, inbox/outbox patterns, or application-level deduplication as appropriate.

### 12.13 Failure, recovery, and replay design

Test these cases before production approval:

1. **One broker stops:** confirm leader election, ISR changes, producer behavior with `acks=all` and `min.insync.replicas=2`, and consumer continuity.
2. **One availability zone fails:** three brokers become unavailable and six remain. Verify rack-aware replica placement leaves two copies of each affected partition in the other AZs, `min.insync.replicas=2` can be satisfied for in-sync partitions, and the six surviving brokers can handle the forecast load and storage budget.
3. **Consumer group is down for four hours:** confirm the configured retention exceeds the outage plus recovery window and that the group can catch up.
4. **Consumer processing is slower than incoming traffic:** measure backlog growth and determine the extra processing capacity required to drain it.
5. **DLQ replay:** test a controlled replay rate so replay does not overwhelm the original topic's consumers or downstream databases.
6. **Rolling broker maintenance:** verify the cluster retains sufficient ISR, disk, CPU, and network capacity during sequential maintenance.
7. **Replica reassignment:** test the effect of moving partitions on network, disk I/O, and client latency.

For backlog recovery, if new data arrives at rate `I` and the consumer fleet can process at rate `C`, backlog drains at `C - I`. If `C <= I`, the backlog never shrinks. Example: with 60 MB/sec incoming and 90 MB/sec processing capacity, a 1.08 TB backlog requires approximately:

```text
Backlog drain time = 1,080,000 MB / (90 - 60) MB/sec
                   = 36,000 sec
                   = 10 hours
```

This estimate assumes steady rates and no downstream bottleneck. Measure the actual catch-up rate during recovery tests.

### 12.14 Growth plan: 24-month scenario

Assume peak traffic and average compressed storage rates grow another 70% beyond the 12-month forecast, and consumer concurrency grows by approximately 50%. Retention stays the same. This is a planning scenario, not a prediction.

**Traffic and storage:**

```text
Future peak write rate = 52.3 × 1.70 = 88.91 MB/sec
Future average compressed disk rate = 7.07 × 1.70 = 12.019 MB/sec
Future primary retained storage = 5.6851 × 1.70 = 9.6647 TB
Future RF3 retained storage = 9.6647 × 3 = 28.9941 TB
With 30% planning allowance = 28.9941 × 1.30 = 37.6923 TB
```

Using the same 7 TB usable broker volume and 70% target utilization after losing one entire AZ (four brokers per AZ in the 24-month, evenly balanced topology):

```text
Surviving brokers required = ceil(37.6923 / (0.70 × 7)) = 8
Total brokers required = 8 + 4 = 12
```

The other capacity checks for a four-broker AZ loss become:

```text
Producer ingress: ceil(88.91 / 15) + 4 = 10 brokers
Consumer egress:  ceil((172.3 × 1.70) / 60) + 4 = 9 brokers
Storage:          12 brokers
```

Storage remains the limiting constraint, so the illustrative 24-month target is 12 brokers, four per availability zone, subject to a new benchmark. With eight brokers surviving an AZ outage, safe benchmark capacity would be 120 MB/sec ingress and 480 MB/sec consumer egress, while 56 TB usable disk would remain. Do not assume that merely adding three brokers automatically redistributes existing partition replicas or leaders; perform controlled reassignment and validate the result.

**Partition growth:** If the same per-partition performance limits remain valid, and group concurrency grows by about 50%, an illustrative future partition plan is:

| Topic | Current partitions | Illustrative 24-month partitions |
|---|---:|---:|
| `orders.events` | 24 | 40 |
| `payments.events` | 12 | 18 |
| `inventory.events` | 12 | 18 |
| `customer.events` | 6 | 9 |
| `notifications.events` | 12 | 18 |
| `orders.dlq` | 6 | 6 |
| `payments.dlq` | 6 | 6 |
| **Total** | **78** | **115** |

At RF3, 115 partitions represent 345 replicas. Across 12 brokers, that is an arithmetic average of 28.75 replicas per broker. Re-run the full per-topic formula with actual future group concurrency and new benchmark data before adopting these counts.

**Important partition migration warning:** Increasing partition count on an existing keyed topic can change the key-to-partition mapping for future records. For entities that require strict ordering across historical and new events, consider creating a versioned topic (for example, `orders.events.v2`), dual-writing or replaying with a controlled cutover, validating consumer state, and retiring the old topic only after migration. Do not increase partitions casually in a stateful production stream.

### 12.15 Security, schema governance, and disaster recovery

A production design also needs controls around who can publish, who can read, and how event contracts evolve.

**Event contract and schema governance**

Include a stable event envelope where appropriate:

```json
{
  "event_id": "unique-event-id",
  "event_type": "OrderConfirmed",
  "schema_version": 1,
  "occurred_at": "2026-11-02T08:30:00Z",
  "aggregate_id": "order-12345",
  "correlation_id": "request-or-workflow-id",
  "data": {}
}
```

Use a schema format and compatibility policy suited to the organization (for example, Avro, Protobuf, or JSON Schema with a schema registry). Define backward/forward compatibility rules before producers and consumers deploy independently. Do not silently change the meaning or type of a field that existing consumers rely on.

**Security**

- Encrypt client and inter-broker traffic with TLS.
- Use an approved authentication mechanism (such as SCRAM or OAuth, according to the platform and identity architecture).
- Apply least-privilege ACLs: producer identities write only to their required topics; consumer identities read only their required topics and consumer groups.
- Use quotas or equivalent client controls to prevent one noisy producer or consumer from monopolizing cluster resources.
- Protect credentials and certificates, rotate them, and audit access.
- Apply encryption at rest through the storage/platform layer where required, and define handling rules for personal or payment-related data.
- Keep development, test, and production identities and data separated.

**Disaster recovery**

Replication factor 3 across availability zones protects against configured in-cluster broker or zone failures; it is not by itself a regional disaster-recovery plan or a backup of every application-side dependency. Define regional RPO/RTO separately. Where cross-region recovery is required, evaluate a supported replication/cluster-linking approach for the Kafka distribution in use, and test topic configuration, schemas, ACLs, consumer offsets, application cutover, and replay behavior. Do not assume that copying topic data alone automatically restores the complete application state.

### 12.16 Final design decision summary

| Design area | 12-month illustrative decision | Why |
|---|---|---|
| Producer applications | 6 applications / 20 producer processes | Based on deployment topology and forecast load |
| Topics | 7 topics: 5 business streams + 2 DLQs | Separate contracts, retention, and failure handling |
| Consumer applications | 18 independent groups / 89 process members | Independent business use cases; members share within group |
| Partitions | 78 total | Based on write throughput, slowest critical group throughput, concurrency, and headroom |
| Replication | RF=3 | Three copies across three racks/AZs where possible |
| Durability baseline | `min.insync.replicas=2`, producer `acks=all` | Reject writes when fewer than two replicas are in sync |
| Brokers | 9 | Sized to preserve the benchmarked workload and storage headroom after a full AZ loss (three brokers unavailable) |
| Broker storage | 7 TB usable per broker | 42 TB remains with one AZ unavailable; planning budget is 22.17 TB |
| KRaft controllers | 3 dedicated nodes | Metadata quorum independent of broker data capacity |
| 12-month average compressed disk rate | 7.07 MB/sec | Measured storage input for retention calculation |
| 12-month peak logical consumer reads | 172.3 MB/sec | All independent groups reading their topic at forecast peak |
| 24-month planning scenario | 12 brokers / 115 partitions | Recalculate storage, throughput, and partition migration before growth |

This example demonstrates why there is no one-to-one mapping among producer processes, consumer processes, partitions, and brokers. Producer process count determines concurrent clients and contributes to aggregate write rate; consumer group size determines desired parallelism; independent consumer groups multiply read demand; partitions provide the parallel units; replication determines copies; retention and compressed input determine disk; and broker count must satisfy the largest of the throughput, partition-placement, storage, and failure-capacity constraints.

## 13. Final production sizing checklist

Use this checklist before approving a Kafka production architecture.

### Workload assessment

- [ ] Measure average and peak messages/sec and MB/sec.
- [ ] Calculate average and maximum message size.
- [ ] Include all producer processes in aggregate throughput.
- [ ] Forecast 12–24 month business growth.
- [ ] Measure each consumer group's processing throughput.

### Topic and partition design

- [ ] Define topics by business domain and data contract.
- [ ] Identify partition key and ordering requirements.
- [ ] Calculate partitions from throughput and consumer parallelism.
- [ ] Include future concurrency and growth headroom.
- [ ] Check total partition replicas across the cluster.

### Cluster sizing

- [ ] Select replication factor and minimum ISR.
- [ ] Calculate retained storage with replication.
- [ ] Benchmark CPU, disk, and network capacity.
- [ ] Calculate brokers with one-broker failure headroom.
- [ ] Distribute replicas across availability zones or racks.
- [ ] Deploy an appropriate KRaft controller quorum.

### Application readiness

- [ ] Use a stable group ID for each logical consumer application.
- [ ] Calculate pods × consumer concurrency per pod.
- [ ] Check maximum active consumers against partitions.
- [ ] Validate producer batching and compression.
- [ ] Test consumer lag recovery and downstream capacity.

### Production readiness

- [ ] Test broker failure and rolling maintenance.
- [ ] Test peak traffic with all consumer groups active.
- [ ] Configure alerts for disk, ISR, latency, and consumer lag.
- [ ] Validate retention against outage and replay requirements.
- [ ] Document scaling and partition migration procedures.

## 14. Recommended order of implementation

1. **Application workload discovery:** Establish producer rate, message sizes, consumer applications, process counts, and traffic growth.
2. **Topic and event architecture:** Define logical topics, ownership, retention, security, and partition keys.
3. **Partition planning:** Calculate throughput requirements and maximum consumer concurrency for every group.
4. **Storage planning:** Calculate compressed data retention, replication, and disk headroom.
5. **Broker and controller planning:** Calculate node count based on benchmark results, partition distribution, and failure tolerance.
6. **Load and failure testing:** Run realistic producer and consumer workloads and test broker loss, consumer recovery, and replay.
7. **Production rollout:** Deploy monitoring, alerts, capacity review, and a documented scale-out procedure.

## 15. Technical references

Use the official documentation for configuration details and version-specific behavior:

1. [Apache Kafka documentation](https://kafka.apache.org/documentation/) — architecture, design, topics, partitions, and configuration.
2. [Apache Kafka broker configuration](https://kafka.apache.org/documentation/#brokerconfigs) — broker settings, replication, and storage-related configuration.
3. [Apache Kafka topic configuration](https://kafka.apache.org/documentation/#topicconfigs) — retention, segment size, and cleanup policy.
4. [Apache Kafka producer configuration](https://kafka.apache.org/documentation/#producerconfigs) — batching, compression, acknowledgements, and idempotence.
5. [Apache Kafka KRaft documentation](https://kafka.apache.org/documentation/#kraft) — controller quorum and metadata management.
6. [Confluent documentation](https://docs.confluent.io/platform/current/) — operational guidance, partition planning, and deployment practices.

---

## Recommended design principles

For a typical production application, use these as initial design principles, then validate them against actual performance tests.

| Design decision | Starting recommendation |
|---|---|
| Broker replication | RF = 3 |
| Minimum in-sync replicas | 2 |
| Producer acknowledgement | `acks=all` |
| Broker availability | At least 3 brokers for RF=3 |
| KRaft controllers | 3 dedicated controllers |
| Topic design | Business-domain oriented |
| Partition count | Throughput + consumer concurrency + growth |
| Consumer groups | One stable group ID per independent application |
| Producer instances | Size by aggregate throughput, not process count |
| Storage | Average compressed ingestion × retention × RF |
| Disk utilization | Maintain free-space and recovery headroom |
| Scaling | Benchmark, monitor, and validate the required N-1 or AZ-loss capacity |

**The biggest architectural mistake to avoid is treating producer processes, consumer processes, partitions, and brokers as one-to-one relationships.** They are independent dimensions. Kafka capacity planning becomes more predictable once each dimension is calculated separately and then validated together.
