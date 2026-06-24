A Log-Structured Merge-tree (LSM tree) is a disk-based data structure designed to provide massive write throughput for transactional and NoSQL databases. It trades traditional read speed in exchange for near-instantaneous, append-only sequential writes.
## How it Works 
LSM trees achieve high performance by separating data into two distinct components: memory (RAM) and disk. 

- Write-Ahead Log (WAL): When a write occurs, it is first appended to a disk-based log for durability (protecting against system crashes).
- MemTable: The write is then recorded in a memory-resident structure (usually a tree) where it is sorted by key. 
- SSTables (Sorted String Tables): Once the MemTable reaches a specific size threshold, it is flushed to disk as an immutable, sequentially sorted file called an SSTable. 
- Compaction: Since new data constantly flushes to new SSTables, a background process continuously merges smaller SSTables into larger ones. This process reclaims disk space, removes deleted data ("tombstones"), and merges overlapping keys.

## Core Trade-offs 
Understanding the trade-offs of the LSM tree architecture is critical to database tuning and selection: 

- Pros: Outstanding write and update throughput, zero write-in-place fragmentation, and scalable performance for high-volume, insert-heavy workloads. 
- Cons: Slower read operations (as the database may have to check multiple SSTables to find the latest key version). Background compaction processes can also occasionally cause CPU and I/O spikes.
