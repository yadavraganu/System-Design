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
12. [End-to-end illustrative production design](#12-end-to-end-illustrative-production-design)
13. [Final production sizing checklist](#13-final-production-sizing-checklist)
14. [Recommended implementation order](#14-recommended-order-of-implementation)
15. [Technical references](#15-technical-references)

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

For one-broker failure tolerance, a simplified throughput estimate is:

```text
N >= ceil(Required cluster workload / Safe capacity per broker) + 1
```

This equation applies to a workload expressed in the same units as the benchmark. Evaluate CPU, network, disk throughput, and partition replicas separately, then take the largest result.

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

## 12. End-to-end illustrative production design

Consider the following application:

| Parameter | Forecast requirement |
|---|---:|
| Producer applications | 4 |
| Producer processes | 12 |
| Current peak ingestion | 40 MB/sec |
| Future peak ingestion | 60 MB/sec |
| Independent consumer groups | 3 |
| Maximum future consumer concurrency | 24 |
| Average compressed stored ingestion | 10 MB/sec |
| Retention | 7 days |
| Replication factor | 3 |
| Minimum ISR | 2 |

Apply the earlier calculations:

| Sizing component | Illustrative result |
|---|---|
| Topics | 1 logical topic for this example |
| Partitions | 36 |
| Partition replicas | 108 |
| Consumer groups | 3 |
| Consumer processing requirement | Up to 24 members in the largest group |
| Logical read throughput | Up to 180 MB/sec across the three groups |
| Retained data before replication | 6.05 TB |
| Retained data with RF=3 | 18.14 TB |
| Provisioned disk estimate | 31.1 TB |
| KRaft controller topology | 3 dedicated controllers as a starting point |

The 36 partitions are calculated using the slowest measured consumer partition throughput, maximum planned concurrency, and headroom.

**Broker count still needs a performance benchmark.** It cannot be conclusively derived from the 31.1 TB storage figure alone. Test the 60 MB/sec ingestion workload together with approximately 180 MB/sec of logical reads, RF=3 replication, and realistic failure conditions.

If you use six brokers in this example, 108 partition replicas average 18 per broker. That is a placement illustration, not a recommendation that six brokers are necessary or sufficient.

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
| Scaling | Benchmark, monitor, and validate N-1 capacity |

**The biggest architectural mistake to avoid is treating producer processes, consumer processes, partitions, and brokers as one-to-one relationships.** They are independent dimensions. Kafka capacity planning becomes more predictable once each dimension is calculated separately and then validated together.
