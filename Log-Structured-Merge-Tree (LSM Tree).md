# Log-Structured Merge-tree (LSM Tree)

## Overview

A Log-Structured Merge-tree (LSM tree) is a disk-based data structure designed to provide massive write throughput for transactional and NoSQL databases. It trades traditional read speed in exchange for exceptional write performance, making it ideal for systems that prioritize high-volume insertions and updates over read latency.

LSM trees are widely used in production databases like:
- **RocksDB** (Facebook)
- **Cassandra** (Apache)
- **HBase** (Apache)
- **LevelDB** (Google)
- **DynamoDB** (Amazon)

---

## How it Works

LSM trees achieve high performance by separating data into two distinct components: memory (RAM) and disk, with a focus on sequential I/O optimization.

### Core Components:

1. **Write-Ahead Log (WAL)**
   - When a write occurs, it is first appended to a disk-based log for durability (protecting against system crashes)
   - Ensures data is never lost even if the system fails mid-operation
   - Acts as a recovery mechanism to replay operations on restart

2. **MemTable**
   - The write is then recorded in a memory-resident structure (usually a balanced tree, B-tree, or skip list) where it is sorted by key
   - Provides fast in-memory access to recent writes
   - Typically implemented as a Red-Black tree or Skip List for ordered traversal
   - Operations are performed in-memory at nanosecond speeds

3. **SSTables (Sorted String Tables)**
   - Once the MemTable reaches a specific size threshold (typically 2-64 MB), it is flushed to disk as an immutable, sequentially sorted file called an SSTable
   - SSTables are stored sequentially on disk, enabling efficient I/O
   - Multiple levels of SSTables exist, creating a hierarchy from newest (Level 0) to oldest (Level N)
   - Each SSTable includes an index and bloom filter for faster lookups

4. **Compaction**
   - Since new data constantly flushes to new SSTables, a background process continuously merges smaller SSTables into larger ones
   - This process reclaims disk space, removes deleted data (tombstones), and consolidates multiple versions of the same key
   - **Compaction Strategies**:
     - **Leveled Compaction**: Maintains strict size ratios between levels (10x is common)
     - **Tiered Compaction**: Merges SSTables only when a level becomes full
     - **Hybrid Strategies**: Combines benefits of both approaches

### Read Path:

1. Check **Bloom Filter** first (fast probabilistic check)
2. Search **MemTable** (fast, in-memory)
3. Check **SSTables** from Level 0 to N (may require multiple SSTable lookups)
4. Return the most recent version of the key

### Write Path:

1. Append to **WAL** on disk (durability guarantee)
2. Insert into **MemTable** (fast, in-memory)
3. Return success to client immediately
4. When MemTable is full → flush to **SSTable** on disk
5. Background **Compaction** merges SSTables

---

## Core Trade-offs

Understanding the trade-offs of the LSM tree architecture is critical to database tuning and selection:

### Advantages (Pros):
- **Outstanding Write Throughput**: Sequential disk writes are much faster than random writes; LSM trees maximize sequential I/O
- **Zero Write-in-Place Fragmentation**: Immutable SSTables prevent fragmentation issues common in B-tree databases
- **Scalable Performance**: Maintains consistent performance for high-volume, insert-heavy workloads
- **Efficient Updates**: In-place updates don't require disk reorganization
- **Simplified Concurrency**: Immutability of SSTables simplifies concurrency control
- **Excellent for Time-Series Data**: Natural fit for databases like InfluxDB, Cassandra

### Disadvantages (Cons):
- **Slower Read Operations**: Database may have to check multiple SSTables to find the latest key version (read amplification)
- **Compaction Overhead**: Background compaction processes can occasionally cause CPU and I/O spikes (write amplification)
- **Space Amplification**: Multiple copies of data exist across levels during compaction
- **Variable Latency**: Reads can have unpredictable latency depending on SSTable count and compaction activity
- **Increased Memory Usage**: Bloom filters and indexes consume additional memory
- **Complex Tuning**: Many parameters (MemTable size, compaction strategy, level ratios) require careful tuning

---

## Performance Characteristics

| Aspect | Performance | Notes |
|--------|-------------|-------|
| **Random Writes** | Excellent (⭐⭐⭐⭐⭐) | Converted to sequential disk I/O |
| **Sequential Writes** | Excellent (⭐⭐⭐⭐⭐) | Natural fit for LSM design |
| **Random Reads** | Fair (⭐⭐⭐) | May require multiple SSTable checks |
| **Sequential Reads** | Good (⭐⭐⭐⭐) | Better than random reads |
| **Updates** | Excellent (⭐⭐⭐⭐⭐) | No in-place modifications |
| **Deletions** | Good (⭐⭐⭐⭐) | Uses tombstones, reclaimed during compaction |

---

## Key Concepts

### Bloom Filters
- Probabilistic data structure that quickly determines if a key **definitely doesn't exist** in an SSTable
- Reduces unnecessary disk I/O for missing keys
- False positives possible, but no false negatives

### Tombstones
- Markers for deleted keys in SSTables
- Actual deletion deferred until compaction
- Enables point-in-time recovery and MVCC (Multi-Version Concurrency Control)

### Write Amplification
- **Definition**: Ratio of data written to disk vs. data written by the application
- **Typical Range**: 5-20x depending on compaction strategy
- **Optimization**: Careful level sizing and compaction algorithm selection

### Read Amplification
- **Definition**: Number of disk I/O operations needed to satisfy a single read request
- **Typical Range**: 1-50+ SSTables to check
- **Optimization**: Bloom filters and compression reduce this significantly

### Space Amplification
- **Definition**: Actual disk usage vs. logical data size
- **Typical Range**: 1-10x due to multiple SSTable copies during compaction
- **Optimization**: Aggressive compaction or compression

---

## Comparison with B-tree

| Feature | LSM Tree | B-tree |
|---------|----------|--------|
| **Writes** | ⭐⭐⭐⭐⭐ Fast | ⭐⭐⭐ Moderate |
| **Reads** | ⭐⭐⭐ Moderate | ⭐⭐⭐⭐⭐ Fast |
| **Space Efficiency** | ⭐⭐⭐ Good | ⭐⭐⭐⭐ Better |
| **Fragmentation** | None | Possible |
| **Compaction** | Continuous | None needed |
| **Use Case** | Write-heavy | Read-heavy |

---

## Tuning Parameters

- **MemTable Size**: Larger = fewer compactions, but more memory used
- **Compaction Level Ratio**: Typically 10x; affects read vs. write amplification
- **Compaction Strategy**: Leveled, Tiered, or Hybrid
- **Bloom Filter Size**: Larger = fewer false positives, more memory
- **Compression**: Reduces space but increases CPU overhead

---

## Real-World Applications

1. **RocksDB**: Embedded key-value store used in production at scale
2. **Cassandra**: Distributed NoSQL database for time-series data
3. **HBase**: Hadoop-based distributed database
4. **Google BigTable**: Inspired LSM tree design
5. **DynamoDB**: AWS's managed NoSQL service
6. **InfluxDB**: Time-series database optimized for metrics

---

## When to Use LSM Trees

✅ **Ideal For:**
- Write-heavy workloads
- Time-series databases
- Event logging systems
- Append-only data structures
- High-volume, bursty writes

❌ **Not Ideal For:**
- Read-heavy, latency-sensitive applications
- Complex transactional queries
- Point lookups with strict SLA requirements
- Data that fits in memory

---

## Conclusion

LSM trees represent a paradigm shift in database design, prioritizing write performance through sequential I/O and immutability. While they introduce complexity in the form of background compaction and variable read latencies, they have become the de facto standard for modern NoSQL and time-series databases where write throughput is paramount.
