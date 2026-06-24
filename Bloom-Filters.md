# Bloom Filters

## Overview

A **Bloom filter** is a space-efficient probabilistic data structure used to test whether an item is a member of a set. It provides constant-time membership testing with minimal memory overhead at the cost of a small probability of false positives.

### Key Characteristics:
- ✅ **Guarantees**: If a Bloom filter says an element is NOT in the set, it's definitely not there (no false negatives)
- ⚠️ **Probabilistic**: If it says the element IS in the set, it might be (false positive possible)
- ✏️ **Immutable**: Elements can be added but not removed from a standard Bloom filter
- 💾 **Space-Efficient**: Uses significantly less memory than hash tables or balanced trees

---

## Why Bloom Filters?

### Problem with Traditional Data Structures

Data structures like `HashSet` work well for small datasets but become expensive for large-scale applications:

| Aspect | HashSet | Bloom Filter |
|--------|---------|--------------|
| **Time Complexity** | O(1) average | O(k) constant (k = # of hash functions) |
| **Space Complexity** | O(n) linear | O(1) constant per element |
| **Memory Overhead** | High | Very Low |
| **False Positives** | None | Possible but tunable |
| **False Negatives** | None | None |

Probabilistic data structures offer **constant time complexity** and **constant space complexity** at the expense of providing an answer that is non-deterministic.

### Use Cases Where Bloom Filters Excel

✅ **Ideal for:**
- **Database Lookups**: Check SSTable membership before expensive disk I/O (RocksDB, LevelDB)
- **Caching**: Reduce cache misses by checking membership before fetching
- **Web Crawling**: Track visited URLs without storing entire URL list
- **Spam Detection**: Efficiently check against known spam lists
- **Network Packet Filtering**: Fast rule matching in firewalls
- **De-duplication**: Identify duplicate elements in streams
- **Search Engines**: Fast document existence checks

---

## How Bloom Filters Work

### Data Structure

A Bloom filter is fundamentally simple:
1. **Bit Array**: A fixed-size array of `m` bits, all initialized to 0
2. **Hash Functions**: `k` independent hash functions that map elements to bit positions

### Core Operations

#### 1. Adding an Item to the Bloom Filter

**Process:**
1. Compute `k` hash values for the element
2. Take modulo `m` (array length) for each hash value to get array indices
3. Set the bits at all `k` indices to 1

**Example:**

Adding `"red"` with 3 hash functions (m = 10):

```
h1(red) mod 10 = 1
h2(red) mod 10 = 3
h3(red) mod 10 = 5
```

Adding `"blue"` with 3 hash functions:

```
h1(blue) mod 10 = 4
h2(blue) mod 10 = 5
h3(blue) mod 10 = 9
```

**Visual Representation:**

![image](https://github.com/yadavraganu/system_design/assets/77580939/35ec443a-4af0-4f04-8255-cd6e2b04ae6c)

Notice that the bucket at position **5** is set to one by both `"red"` and `"blue"`. This is a normal occurrence due to hash collisions.

#### 2. Testing Membership

**Process:**
1. Compute `k` hash values for the test element
2. Take modulo `m` for each hash value
3. Check if **ALL** bits at those indices are set to 1

**Result Interpretation:**
- **All bits = 1**: Element **might be** in the set (false positive possible)
- **Any bit = 0**: Element is **definitely NOT** in the set (no false negatives)

**Example - Testing for Membership:**

The bloom filter is queried to check the membership of `"blue"`. The buckets that should be checked are identified by:

```
h1(blue) mod 10 = 4
h2(blue) mod 10 = 5
h3(blue) mod 10 = 9
```

All bits at positions 4, 5, and 9 are set to 1 → **"blue" might be a member** ✓

The bloom filter is queried to check the membership of `"black"`, which was never added:

```
h1(black) mod 10 = 0
h2(black) mod 10 = 3
h3(black) mod 10 = 6
```

The bit at position **0** is set to **0** → **"black" is definitely NOT a member** ✗

The verification of the remaining bits can be skipped since we found a zero.

---

## Mathematical Analysis

### False Positive Rate

The false positive rate depends on:
- **m**: Total bits in the array
- **n**: Number of elements added
- **k**: Number of hash functions

**Optimal number of hash functions:**
```
k = (m / n) × ln(2) ≈ 0.693 × (m / n)
```

**False positive probability:**
```
P ≈ (1 - e^(-kn/m))^k
```

### Space Complexity

For a desired false positive rate P:
```
m ≈ -n × ln(P) / (ln(2)²)
```

**Space per element:** ~1.44 bits per element for 1% false positive rate (vs. ~100+ bits for a HashSet)

### Time Complexity

| Operation | Time | Space |
|-----------|------|-------|
| **Insert** | O(k) | O(m) |
| **Lookup** | O(k) | O(1) per lookup |
| **Total Space** | - | O(m) |

Where k = number of hash functions, m = bit array size

---

## Performance Characteristics

| Aspect | Performance | Notes |
|--------|-------------|-------|
| **Random Writes** | O(k) | Hash functions compute in parallel |
| **Lookups** | O(k) | Extremely fast for membership testing |
| **Space Efficiency** | ⭐⭐⭐⭐⭐ | ~1.4 KB per 10K items @ 1% FP rate |
| **False Positives** | Tunable | Controllable via bit array size |
| **False Negatives** | None | 0% - guaranteed accuracy |

---

## Variants and Extensions

### 1. Counting Bloom Filter
- **Problem**: Standard Bloom filters cannot delete elements
- **Solution**: Replace each bit with a small counter (4-8 bits)
- **Trade-off**: 4-8x higher memory for delete capability
- **Use case**: Dynamic sets requiring deletions

### 2. Scalable Bloom Filter
- Dynamically adds new filter levels as elements increase
- Maintains false positive rate independently of insertion count
- **Use case**: Unbounded data streams

### 3. Partitioned Bloom Filter
- Divides bit array into k partitions
- Each hash function maps to one partition
- Better cache locality and parallelism
- **Use case**: Distributed systems and GPU acceleration

### 4. Cuckoo Filter
- Uses cuckoo hashing instead of hash functions
- Supports deletion with standard approach
- Often smaller than Bloom filters for same FP rate
- **Use case**: When deletion is critical and memory is tight

---

## Real-World Applications

### Database Systems
- **RocksDB**, **LevelDB**: Check SSTable membership before disk reads
- **Cassandra**, **HBase**: Fast key existence checks
- **BigTable**: Reduce unnecessary disk accesses
- **InfluxDB**: Time-series data lookups

### Search Engines
- **Google Search**: Track indexed pages
- **Web Crawlers**: Identify visited URLs without storing all URLs
- **Elasticsearch**: Filter operations on large indexes

### Network & Security
- **Packet Filtering**: Fast rule matching in firewalls
- **Spam Detection**: Check against known spam lists
- **DDoS Protection**: Track attacking IPs
- **DNS Systems**: Negative caching for non-existent domains

### Stream Processing
- **Kafka Streams**: De-duplicate event streams
- **Flink**: Fast membership testing for state
- **Spark Streaming**: Remove duplicate messages

### Content Delivery
- **CDN Cache**: Determine if content exists before fetch
- **DNS Caching**: Fast negative response for non-existent domains

---

## Bloom Filters vs. Other Data Structures

| Feature | Bloom Filter | HashTable | B-Tree | Cuckoo Filter |
|---------|--------------|-----------|--------|---------------|
| **Lookup Time** | O(k) | O(1) avg | O(log n) | O(1) avg |
| **Space Efficiency** | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **False Positives** | Yes (tunable) | No | No | Yes (tunable) |
| **Deletions** | No* | Yes | Yes | Yes |
| **Ordered Traversal** | No | No | Yes | No |
| **Worst Case** | O(k) | O(n) | O(n) | O(n) |
| **Memory per item** | 1-2 bits | ~100 bits | ~128 bits | 2-4 bits |

*Counting Bloom Filter supports deletions

---

## Implementation Considerations

### Choosing Parameters

**m (Bit Array Size):**

For n elements and P (false positive rate):
```
m = -n × log(P) / (log(2)²)
```

Example: For 1 million elements and 1% false positive rate:
```
m ≈ -1,000,000 × log(0.01) / (log(2)²)
m ≈ 9.6 million bits ≈ 1.2 MB
```

**k (Number of Hash Functions):**

Optimal number of hash functions:
```
k = (m / n) × log(2) ≈ 0.693 × (m / n)
```

For the example above: k ≈ 7 hash functions

### Hash Function Selection

- Use **independent, high-quality hash functions**
- Common choices: MurmurHash, xxHash, SHA-1
- Generate k functions from one base function using salt values

---

## Advantages vs. Disadvantages

### ✅ Advantages

| Advantage | Benefit |
|-----------|---------|
| **Extremely Space-Efficient** | 1-2 bits per element vs. HashSet's ~100+ bits |
| **Constant Time Complexity** | O(k) operations, independent of set size |
| **No False Negatives** | If "not in set", it's guaranteed |
| **Highly Parallelizable** | Hash functions can be computed in parallel |
| **Predictable Memory Usage** | Fixed space regardless of set growth |
| **Fast Negative Lookups** | Quickly confirms non-membership |

### ❌ Disadvantages

| Disadvantage | Impact |
|--------------|--------|
| **False Positives** | Need secondary verification for some use cases |
| **No Deletions (Standard)** | Can only add elements (use Counting BF for delete) |
| **Not Reversible** | Cannot recover original elements from filter |
| **Hash Function Dependent** | Quality matters; poor hash functions increase false positives |
| **No Ordering** | Cannot iterate or find closest element |
| **Tuning Required** | Need to select m and k parameters correctly |

---

## When to Use Bloom Filters

### ✅ Good Fit:
- Membership testing on massive datasets
- Memory is limited or expensive
- False positives are acceptable and can be verified
- Read-heavy workloads with rare writes
- Distributed systems needing efficient serialization
- Database optimization (avoiding expensive disk I/O)

### ❌ Poor Fit:
- Need guaranteed accuracy (no false positives tolerable)
- Frequent deletions required
- Need to retrieve original elements
- Small datasets (use HashSet or HashMap instead)
- Complex range queries or ordering needed

---

## Summary

Bloom filters are an elegant probabilistic solution for **fast, space-efficient membership testing**. While they sacrifice absolute accuracy for massive memory savings, they're invaluable in systems handling large-scale data where occasional false positives are acceptable and memory is at a premium.

**Key Takeaway**: Trade a small, controllable false positive rate for dramatic space efficiency—a worthwhile exchange for many real-world applications.

