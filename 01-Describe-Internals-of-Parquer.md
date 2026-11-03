# 1. Learning objectives and mental model

After working through this guide, you should be able to:

- Explain why columnar storage is effective for analytical workloads.
- Describe the relationship between a Parquet file, row group, column chunk, and page.
- Explain how a reader uses the footer and column statistics to avoid unnecessary I/O.
- Distinguish physical types from logical types.
- Explain definition levels and repetition levels for nullable and nested data.
- Compare encoding with compression and select options based on data characteristics.
- Diagnose whether a query is benefiting from partition pruning, column pruning, row-group pruning, or page-level skipping.
- Write and inspect Parquet using PyArrow and PySpark.
- Design and inspect multi-file Parquet datasets, including partitioned layouts.
- Troubleshoot small files, ineffective pruning, schema incompatibility, and poor scan performance.

## The mental model to remember

Parquet is a **columnar file format**, not a query engine and not a table format. Its performance comes from the combination of:

1. **Column-oriented layout** — read only the columns a query needs.
2. **Encoding** — represent values efficiently before compression.
3. **Compression** — reduce the bytes that must be stored and transferred.
4. **Metadata** — describe the schema and the location and properties of stored data.
5. **Data organization** — arrange files, row groups, and values so irrelevant data can be skipped.

A useful hierarchy is:

```text
Dataset / Table
└── Parquet files
    ├── File header
    ├── Row group 1
    │   ├── Column chunk: column A
    │   │   ├── Dictionary page (optional)
    │   │   └── Data pages
    │   ├── Column chunk: column B
    │   │   └── Data pages
    │   └── ...
    ├── Row group 2
    │   └── ...
    ├── File metadata (footer)
    ├── Metadata length
    └── File trailer: PAR1
```

**Important distinction:** partitioning belongs to the dataset/table layout outside an individual Parquet file. Row groups, column chunks, and pages belong to the file format itself.

# 2. What Parquet is—and what it is not

## 2.1 Definition

Apache Parquet is an open-source, column-oriented file format designed for efficient storage and retrieval of structured and nested data. It is commonly used for analytical storage in data lakes, warehouses, and distributed processing systems.

Parquet is supported by a broad ecosystem of readers and writers, including Apache Spark, Apache Hive, Apache Flink, Trino, Presto, DuckDB, and Apache Arrow/PyArrow. Feature support and defaults can differ by implementation and version.

## 2.2 What Parquet provides

- A self-describing file schema.
- Column-oriented storage inside row groups.
- Page-level value encoding.
- Optional compression of page data.
- File and column-chunk metadata.
- Statistics such as minimum and maximum values when written and supported.
- Support for nested structures and nullable fields.
- Optional page indexes, Bloom filters, and other advanced metadata features.

## 2.3 What Parquet does not provide by itself

Parquet does **not** inherently provide:

- A SQL query engine.
- A transaction manager for a multi-file table.
- Atomic multi-file commits.
- A catalog or governance layer.
- A complete change-data-capture or upsert model.
- A guarantee that every reader supports every optional feature.

Capabilities such as snapshots, atomic multi-file commits, concurrent-writer coordination, time travel, and managed table-level schema evolution require a separate table-management layer. That layer can use Parquet as its underlying data file format.

# 3. Row storage versus columnar storage

Consider this employee dataset:

| employee_id | name | department | salary |
|---:|---|---|---:|
| 101 | Asha | Engineering | 90000 |
| 102 | Ravi | Finance | 75000 |
| 103 | Mei | Engineering | 95000 |
| 104 | Omar | HR | 65000 |

## 3.1 Row-oriented representation

A row-oriented representation stores fields for each record together:

```text
101, Asha, Engineering, 90000
102, Ravi, Finance, 75000
103, Mei, Engineering, 95000
104, Omar, HR, 65000
```

This is a natural layout for workloads that frequently retrieve or update complete records. Examples include many transactional workloads and event serialization formats.

## 3.2 Column-oriented representation

A columnar layout groups values by column:

```text
employee_id: 101, 102, 103, 104
name:        Asha, Ravi, Mei, Omar
department:  Engineering, Finance, Engineering, HR
salary:      90000, 75000, 95000, 65000
```

Consider an analytical query:

```sql
SELECT department, AVG(salary)
FROM employees
GROUP BY department;
```

The engine needs `department` and `salary`, not `name` or `employee_id`. A columnar reader can avoid reading the unused column chunks.

## 3.3 Why columnar storage helps analytics

| Benefit | Why it matters |
|---|---|
| Column pruning | Avoids reading columns not referenced by the query. |
| Better compression opportunities | Values of the same type and similar distribution are stored together. |
| Predicate pushdown and pruning | Metadata can help reject row groups or pages that cannot match a filter. |
| Vectorized execution | Engines can decode and process batches of column values efficiently. |
| Parallel reads | Independent column chunks and row groups can be read in parallel where the engine and storage support it. |

Columnar storage is not automatically faster for every workload. A query that retrieves every column for a tiny number of individual records may not benefit as much as a large scan-and-aggregate query.

# 4. The physical structure of a Parquet file

## 4.1 File layout

A standard Parquet file begins and ends with the four-byte magic value `PAR1`. Between them are column chunks followed by the file metadata and its length.

Conceptually:

```text
+------------------------------------------+
| PAR1                                     | 4-byte header
+------------------------------------------+
| Row group 1                              |
|   Column chunk A                         |
|   Column chunk B                         |
|   Column chunk C                         |
+------------------------------------------+
| Row group 2                              |
|   Column chunk A                         |
|   Column chunk B                         |
|   Column chunk C                         |
+------------------------------------------+
| ...                                      |
+------------------------------------------+
| File metadata (serialized metadata)      |
+------------------------------------------+
| Metadata length (4-byte little-endian)   |
+------------------------------------------+
| PAR1                                     | 4-byte trailer
+------------------------------------------+
```

The footer is written after the data. This allows a writer to emit data in a single pass and then write the metadata needed by readers.

A reader typically seeks to the end of the file, reads the trailer and metadata length, loads the footer, and uses the metadata to locate column chunks.

## 4.2 File

A Parquet file is the physical container. It includes data pages, metadata, and the schema for that file. A logical dataset or table usually consists of many Parquet files.

## 4.3 Row group

A **row group** is a logical horizontal partition of the rows. A row group contains one column chunk for each column in the schema.

Example for a dataset containing 10 million rows:

```text
Row group 1: a range of rows from the dataset
Row group 2: the next range of rows
Row group 3: the next range of rows
...
```

A row group is a logical Parquet structure; it is not guaranteed to be the same thing as a storage block, an upload part, a Spark partition, or an execution task.

Row groups influence:

- The granularity of metadata-based pruning.
- Read parallelism available to a reader.
- Writer buffering and memory requirements.
- Compression effectiveness and metadata overhead.

## 4.4 Column chunk

A **column chunk** contains the data for one column within one row group. The pages of a column chunk are stored contiguously in the file.

For a row group with columns `employee_id`, `department`, and `salary`, there are three column chunks—one for each column.

Column chunks are the primary units of column-specific I/O. If a query needs only `salary`, the reader generally does not need to fetch the `name` or `department` chunks.

## 4.5 Page

A **page** is the conceptual unit of encoding and compression inside a column chunk. A column chunk contains one or more pages.

Common page types include:

- **Data Page V1** — stores encoded values and, when applicable, repetition and definition levels.
- **Data Page V2** — a newer data-page layout with separate handling of level sections and encoded values.
- **Dictionary Page** — stores dictionary entries for a dictionary-encoded column chunk.

A dictionary page, when present, occurs before the data pages that use it. A column chunk can use dictionary encoding for some pages and plain encoding for others if the writer falls back from dictionary encoding.

Do not confuse **page indexes** with **index pages**. Page indexes are optional metadata structures that help locate relevant data pages; they are not ordinary data pages.

## 4.6 Which level does what?

| Structure | Main concern | Typical optimization role |
|---|---|---|
| Dataset partition | Coarse-grained data organization | Partition pruning |
| File | Physical object and scheduling unit | File discovery and parallel reads |
| Row group | Horizontal group of rows | Statistics-based row-group skipping |
| Column chunk | One column in one row group | Column pruning and column I/O |
| Page | Encoded/compressed data unit | Fine-grained reading and page skipping |

# 5. How a Parquet reader works

Consider:

```sql
SELECT employee_id, salary
FROM employees
WHERE salary > 90000;
```

A simplified read lifecycle is:

1. **Discover files.** The engine identifies the files belonging to the table or dataset, applying partition pruning where possible.
2. **Read file metadata.** For each selected file, it reads the footer and obtains the schema, row groups, column chunks, offsets, encodings, compression, and available statistics.
3. **Resolve the requested schema.** The engine maps the query's logical columns to Parquet fields and handles any supported schema conversion.
4. **Apply column pruning.** It selects `employee_id` and `salary`; other column chunks can be avoided.
5. **Evaluate available pruning metadata.** For `salary > 90000`, a row group whose maximum salary is 85000 cannot match and can be skipped.
6. **Read relevant column chunks and pages.** The engine fetches the necessary byte ranges from storage.
7. **Decompress and decode.** It reverses the page compression and value encodings.
8. **Apply the filter and execute the query.** The engine evaluates actual values, performs projections, joins, or aggregations, and returns results.

## 5.1 Example of row-group skipping

Suppose row-group statistics are:

| Row group | Minimum salary | Maximum salary | Can contain `salary > 90000`? |
|---|---:|---:|---|
| RG1 | 30000 | 70000 | No — skip |
| RG2 | 50000 | 85000 | No — skip |
| RG3 | 60000 | 120000 | Yes — read relevant data |
| RG4 | 100000 | 150000 | Yes — read relevant data |

Statistics prove that RG1 and RG2 cannot satisfy the predicate. They do **not** prove that every row in RG3 or RG4 satisfies it; the engine still evaluates the filter on actual values.

## 5.2 Important limitation

The format can store metadata, but whether a query benefits from it depends on the writer having produced useful metadata and on the reader supporting and using that metadata. Always verify with the target engine's query plan and scan metrics.

# 6. Physical types, logical types, and schema

Parquet separates the low-level **physical type** used to store a value from the **logical type** that describes how the value should be interpreted.

## 6.1 Common physical types

| Physical type | Basic representation |
|---|---|
| `BOOLEAN` | Boolean value, bit-packed in plain encoding |
| `INT32` | 32-bit integer |
| `INT64` | 64-bit integer |
| `INT96` | Legacy 12-byte type, commonly encountered for older timestamps |
| `FLOAT` | 32-bit IEEE floating point |
| `DOUBLE` | 64-bit IEEE floating point |
| `BYTE_ARRAY` | Length-prefixed variable-length byte sequence in plain encoding |
| `FIXED_LEN_BYTE_ARRAY` | Fixed-width byte sequence |

`INT96` is a legacy, non-standard type. For new timestamp designs, prefer standard timestamp logical annotations backed by `INT64`, after checking compatibility with all readers in the platform.

## 6.2 Common logical types

| Logical type | Common physical representation | Meaning |
|---|---|---|
| String | `BYTE_ARRAY` | UTF-8 string |
| Date | `INT32` | Days from the Unix epoch |
| Timestamp | `INT64` | Time value with a declared unit and timestamp semantics |
| Decimal | `INT32`, `INT64`, `BYTE_ARRAY`, or `FIXED_LEN_BYTE_ARRAY` | Exact decimal with precision and scale |
| UUID | `FIXED_LEN_BYTE_ARRAY` | 16-byte UUID representation |
| Time | `INT32` or `INT64` | Time of day with a declared unit |

The logical annotation matters for interoperability. An `INT64` alone does not tell a consumer whether the value is an identifier, a count, or a timestamp.

## 6.3 Decimal: precision and scale

For `DECIMAL(18, 2)`:

- Precision 18 means at most 18 total decimal digits.
- Scale 2 means two digits to the right of the decimal point.
- The value `12345.67` has precision 7 and scale 2.

Use decimal types for currency and exact financial quantities. Binary floating-point types such as `FLOAT` and `DOUBLE` are approximate and can introduce rounding behavior.

## 6.4 Timestamp semantics

Timestamp interoperability needs explicit decisions:

- Is the value an absolute instant or a local wall-clock time?
- Is the unit milliseconds, microseconds, or another supported unit?
- Is timezone adjustment represented in the logical annotation?
- Does every writer and reader interpret the annotation consistently?

A reliable data contract should specify timestamp timezone semantics and precision. Test timestamps around daylight-saving transitions and values with sub-second precision when relevant.

# 7. Nested data: definition and repetition levels

Parquet supports nested structures such as structs, lists, and maps. It does not need to store each nested object as a JSON string. Instead, it uses a schema plus **definition levels** and **repetition levels** to represent optionality and repetition in a columnar layout.

## 7.1 Definition levels

A definition level indicates how much of a nested path is present. It distinguishes a missing or null optional value from a present value.

Conceptually, for an optional field:

```text
employee
└── optional address
    └── optional city
```

A row may have:

- No `address`.
- An `address` but no `city`.
- An `address.city` value.

Definition levels preserve these distinctions without requiring a physical placeholder value for every null.

## 7.2 Repetition levels

A repetition level indicates whether a value begins a new repeated group or continues a repeated field. It is essential for representing lists and repeated nested structures.

Example source record:

```json
{
  "employee_id": 101,
  "skills": ["Python", "SQL", "Spark"]
}
```

The repeated `skills` values are represented with repetition information so the reader can reconstruct the list boundaries.

## 7.3 Interview distinction

- **Definition level:** How much of the optional/nested path is defined?
- **Repetition level:** Does this value start a new repeated instance or continue a repeated structure?

These levels are associated with the nested column path. They are not ordinary user data columns.

# 8. Encoding techniques

Encoding transforms values into a representation that may require fewer bits or expose patterns to compression. Encoding is distinct from compression.

## 8.1 Plain encoding

Plain encoding stores values in their basic physical representation. It is the simplest general-purpose encoding and is used when other encodings are unsuitable or when a writer chooses not to use them.

Example:

```text
Values: 100, 200, 300, 400
Plain:  each integer in its physical binary representation
```

Plain encoding is not necessarily inefficient; for high-cardinality or irregular data it can be a reasonable choice.

## 8.2 Dictionary encoding

Dictionary encoding stores distinct values in a dictionary page and stores integer dictionary IDs in data pages.

Original values:

```text
Engineering, Finance, Engineering, HR, Finance, Engineering
```

Dictionary:

```text
0 = Engineering
1 = Finance
2 = HR
```

IDs:

```text
0, 1, 0, 2, 1, 0
```

This is often effective for low-cardinality columns such as status, country, department, or product category.

Limitations:

- High-cardinality columns can create large dictionaries.
- Random unique identifiers often do not benefit much.
- Writers can stop dictionary encoding and fall back to plain encoding when the dictionary becomes too large.
- Dictionary encoding decisions are made per column chunk; do not assume every page or file uses it.

## 8.3 Run-length encoding and bit-packing hybrid

Run-length encoding represents consecutive repeated values compactly. Parquet's RLE/bit-packing hybrid is used in contexts such as repetition levels, definition levels, and dictionary indices.

Conceptual example:

```text
1, 1, 1, 1, 1, 2, 2, 2
```

Could be represented conceptually as:

```text
(1 repeated 5 times), (2 repeated 3 times)
```

The actual Parquet hybrid encoding also uses bit-packing for runs where that is more efficient.

## 8.4 Delta binary packed encoding

Delta encoding stores a first value followed by differences between adjacent integer values.

Example:

```text
Original:  1000, 1005, 1010, 1015, 1020
Deltas:    first value 1000, then +5, +5, +5, +5
```

Parquet's `DELTA_BINARY_PACKED` encoding supports `INT32` and `INT64`. It can be useful when differences are small or have a compact distribution, such as sequential counters and some timestamp data.

## 8.5 Delta length byte-array encoding

`DELTA_LENGTH_BYTE_ARRAY` encodes the lengths of byte arrays using delta encoding, followed by the concatenated byte-array values. It can help with variable-length strings and binary values.

## 8.6 Delta byte-array encoding

`DELTA_BYTE_ARRAY` uses common prefixes between adjacent byte-array values. It is also known as front coding or incremental encoding.

Example:

```text
customer-1001
customer-1002
customer-1003
```

Rather than repeating the common prefix for every value, the encoding can store a prefix length and the remaining suffix.

This is most useful when adjacent values share prefixes; sorting or clustering can make such patterns more common.

## 8.7 Byte-stream split

Byte-stream split rearranges the bytes of fixed-width values into separate byte streams. It does not reduce the raw number of bytes on its own, but it can improve the compression ratio and speed when a codec is applied afterward. It supports types including floating-point and integer types and fixed-length byte arrays.

## 8.8 Encoding summary

| Encoding | Typical fit | Important note |
|---|---|---|
| Plain | General values; fallback | Simple physical representation |
| Dictionary | Low-cardinality repeated values | Can fall back to plain |
| RLE/bit-packing hybrid | Levels and dictionary IDs | Combines repeated runs and packed values |
| Delta binary packed | `INT32` / `INT64` with useful delta patterns | Stores differences in blocks |
| Delta length byte array | Variable-length byte arrays | Encodes lengths as deltas |
| Delta byte array | Similar adjacent strings | Uses common prefixes |
| Byte-stream split | Fixed-width numeric/binary values | Most useful with compression |

A writer selects encodings based on its implementation and options. Validate actual output by inspecting metadata rather than assuming a specific encoding from the data type alone.

# 9. Compression codecs

Compression reduces the byte size of encoded page data. In a simplified view:

```text
Logical values
    ↓
Encoding
    ↓
Encoded page data
    ↓
Compression codec
    ↓
Compressed page bytes
```

The exact page layout and treatment of level sections depend on the page version and format rules, so this diagram is a conceptual pipeline rather than a byte-for-byte description of every page type.

## 9.1 Common codecs

| Codec | General characteristics | Consider when |
|---|---|---|
| Uncompressed | No codec CPU cost; largest files in many cases | Debugging or CPU-bound cases where tests show it helps |
| Snappy | Usually fast compression and decompression; moderate compression ratio | General-purpose analytics and balanced throughput |
| GZIP | Often stronger compression but more CPU-intensive | Storage or transfer reduction is more important than CPU time |
| Zstandard (ZSTD) | Configurable levels; often a strong size/speed balance | Benchmarking modern analytical workloads |
| LZ4 / LZ4_RAW | Focus on fast processing | Workloads prioritizing speed; verify precise codec compatibility |
| Brotli | Can compress text-like data well; CPU profile varies | Workloads and readers with compatible support |

Codec support is implementation- and version-dependent. Check both the writer and every important reader before standardizing on a codec.

## 9.2 Encoding versus compression

| Encoding | Compression |
|---|---|
| Changes how values are represented | Compresses a byte stream |
| Examples: dictionary, delta, RLE | Examples: Snappy, GZIP, ZSTD |
| Can exploit structure in the values | Exploits repetition and patterns in encoded bytes |
| Happens before page compression in the conceptual write path | Applied to the page data according to the format/page version |

## 9.3 How to choose a codec

Do not choose only by compressed file size. Benchmark:

- Write throughput.
- Read throughput.
- CPU utilization.
- Compressed bytes stored.
- Bytes scanned by the query engine.
- End-to-end query duration.

A smaller file can be slower if decompression consumes more CPU. A fast codec can be more expensive overall if it significantly increases storage and scan I/O.

# 10. Metadata, statistics, indexes, and Bloom filters

Parquet's metadata is central to its read performance.

## 10.1 File metadata

The footer describes the file schema, row count, row groups, column chunks, encodings, compression, offsets, sizes, and available statistics.

It allows a reader to find the bytes for required columns without scanning the entire file from the beginning.

## 10.2 Column-chunk statistics

Depending on the writer and type, statistics can include:

- Minimum value.
- Maximum value.
- Null count.
- Number of values.

Example:

```text
Row group 1, column salary:
    min = 30000
    max = 70000
    null_count = 12
```

For `salary > 90000`, that row group cannot contain a match and can be skipped.

Statistics must be interpreted carefully for logical types, sort order, nulls, and truncated values. A reader must be conservative: metadata should never cause valid matching rows to be skipped.

## 10.3 Page index

A page index is optional metadata that stores page-level information. It has two important structures:

- **Column index:** page-level value bounds and related information used to identify pages that may match a predicate.
- **Offset index:** locations and row information used to seek to the relevant pages and align reads across columns.

Page indexes can make selective scans more efficient than row-group-only pruning. They are especially useful when a row group contains many pages and the predicate is selective.

Support is not universal or identical across all engines. A writer emitting a page index does not guarantee that a particular reader will use it.

## 10.4 Bloom filters

A Bloom filter is a compact probabilistic membership structure associated with a column chunk.

It can answer:

- **Definitely not present.** The reader can safely skip the chunk for that equality lookup.
- **Possibly present.** The reader must inspect the data to determine whether the value exists.

A Bloom filter may return false positives, but it must not return false negatives when correctly implemented.

Bloom filters can be useful for high-cardinality columns where a large dictionary would be expensive and min/max statistics are not selective enough—for example, point lookups on an identifier.

Bloom-filter effectiveness depends on writer support, filter configuration, reader support, predicate shape, and data layout.

## 10.5 Statistics versus dictionary versus Bloom filter

| Mechanism | What it can tell the reader | Typical strength |
|---|---|---|
| Min/max statistics | A value is outside the recorded range | Effective for range predicates and ordered data |
| Dictionary | A value is not among the dictionary entries | Potentially selective for dictionary-encoded columns |
| Bloom filter | A value is definitely absent or possibly present | Useful for equality predicates on high-cardinality columns |
| Page index | Which pages may contain values in a predicate range | Fine-grained page skipping |

# 11. Query optimization: pruning and pushdown

These terms are related but not interchangeable.

## 11.1 Partition pruning

Partition pruning skips entire dataset directories or partition values based on predicates on partition columns.

Example layout:

```text
orders/
  year=2025/month=12/...
  year=2026/month=01/...
  year=2026/month=02/...
```

Query:

```sql
SELECT customer_id, amount
FROM orders
WHERE year = 2026 AND month = 1;
```

If `year` and `month` are registered as partition columns, the engine can avoid unrelated partitions.

## 11.2 Column pruning

Column pruning avoids reading column chunks that are not required by the query.

```sql
SELECT customer_id, amount
FROM orders;
```

The reader does not need unrelated columns such as `shipping_address` or `marketing_attributes`.

## 11.3 Predicate pushdown

Predicate pushdown passes supported filters closer to the data source. A Parquet reader can use file metadata and statistics to avoid reading row groups or pages that cannot satisfy the predicate.

## 11.4 Row-group pruning

A row group is skipped when available metadata proves it cannot contain a qualifying row.

## 11.5 Page-level skipping

When page indexes are written and supported by the reader, the reader may skip individual pages inside a column chunk.

## 11.6 A query can use several optimizations together

```text
SQL query
   ↓
Partition pruning       → skip partition directories / files
   ↓
Column pruning          → avoid unneeded column chunks
   ↓
Row-group statistics    → skip row groups
   ↓
Dictionary / Bloom      → reject chunks for supported predicates
   ↓
Page index              → skip eligible pages
   ↓
Decode qualifying data  → execute remaining filters and operators
```

Not every engine implements every step, and execution order varies. Confirm the physical plan and runtime metrics rather than relying only on SQL text.

# 12. File layout, row-group sizing, and the small-file problem

## 12.1 File size and row-group size are different controls

- **File size** is the size of the complete Parquet object, often compressed.
- **Row-group size** controls the amount of data in a row group. It may be configured in rows or bytes depending on the writer.
- **Page size** controls the approximate target size of encoded data pages.

A file can contain multiple row groups. One large file is not necessarily one large row group.

## 12.2 Small-file problem

Example:

```text
1,000,000 files × 100 KB each
```

This creates overhead in file listing, metadata reads, query planning, scheduling, object-store requests, and task management. Many tiny files can make a query slow even when the total data volume is not large.

Typical remedies:

- Compact files into fewer, larger files.
- Avoid excessive output partitions.
- Avoid partitioning by very high-cardinality columns.
- Tune Spark shuffle and output partition counts.
- Use a scheduled compaction process for continuously written datasets.

## 12.3 Very large files and row groups

Large files and row groups can reduce metadata overhead, but may also:

- Reduce available parallelism.
- Increase buffering and memory requirements for writers.
- Make pruning less granular.
- Increase the amount of data read for selective predicates.

There is no universal best file or row-group size. As an initial experiment, many lake workloads benchmark compressed files in the low-hundreds-of-megabytes range, then adjust based on engine, object-store behavior, workload, and concurrency. Treat this as a starting point—not a Parquet requirement or guarantee.

## 12.4 Tune using evidence

Measure:

- Number of files per partition.
- Compressed file-size distribution (minimum, median, p95, maximum).
- Row groups per file.
- Rows and uncompressed bytes per row group.
- Query planning time.
- Scan bytes and task count.
- Executor memory and spill.
- Read/write throughput.

A good layout balances scan efficiency, write throughput, pruning, parallelism, and operational cost.

# 13. Partitioning, sorting, and data clustering

## 13.1 Partitioning

Partition columns should be selected from real query patterns and expected data volume.

Good candidates often include:

- Event date at an appropriate granularity.
- Transaction month.
- Region, when it has a manageable number of values and meaningful query selectivity.

Poor candidates often include:

- Unique transaction IDs.
- Customer IDs with millions of values.
- Random UUIDs.
- High-precision timestamps that create many tiny partitions.

A partition column should eliminate meaningful amounts of data without creating an unmanageable number of directories or tiny files.

## 13.2 Sorting within files or row groups

If queries frequently filter by `event_time`, sorting data by `event_time` before writing can make min/max statistics more selective.

Random distribution:

```text
RG1 event_time range: Jan 01 - Dec 31
RG2 event_time range: Jan 01 - Dec 31
RG3 event_time range: Jan 01 - Dec 31
```

Time-clustered distribution:

```text
RG1 event_time range: Jan 01 - Jan 10
RG2 event_time range: Jan 11 - Jan 20
RG3 event_time range: Jan 21 - Jan 31
```

For a query restricted to Jan 21–31, the second layout may let a reader skip RG1 and RG2.

Sorting costs CPU and often a shuffle in distributed systems. It can also create skew if the distribution is highly uneven. Sort only when the workload benefits justify the write cost.

## 13.3 Sorting metadata is not the same as sorting data

Some writers can record sorting-column metadata. Recording a sort order does not itself sort or validate the rows. The pipeline must actually produce data in the declared order; inaccurate metadata can lead to incorrect assumptions by readers that rely on it.

# 14. Schema evolution and interoperability

## 14.1 Schema evolution

A Parquet file embeds a schema, but a dataset usually contains many files written at different times. Readers must reconcile those file schemas.

Examples:

Version 1:

```text
employee_id: INT64
name: STRING
salary: DECIMAL(18,2)
```

Version 2:

```text
employee_id: INT64
name: STRING
salary: DECIMAL(18,2)
department: STRING (optional)
```

Adding an optional field is often manageable, but compatibility depends on the reader's schema-resolution behavior and the table contract.

## 14.2 Common schema changes

| Change | Risk / consideration |
|---|---|
| Add optional field | Often compatible if readers handle absent fields as null |
| Add required field | Existing files do not contain a value; needs a default or migration strategy |
| Remove field | Old files may still contain it; consumers may depend on it |
| Rename field | Often appears as one field removed and another added unless stable field IDs and reader semantics are defined and consistently supported |
| Widen integer type | Reader-specific compatibility must be checked |
| String to integer | Usually a breaking semantic/type change |
| Change decimal precision or scale | Validate representable values and reader compatibility |
| Change timestamp unit or timezone semantics | High risk of precision or interpretation errors |

## 14.3 Practical controls

- Define a canonical schema and data contract.
- Validate incoming schemas before writing.
- Avoid uncontrolled type inference in production pipelines.
- Test old and new files together with every important reader.
- Make nullability and decimal/timestamp semantics explicit.
- Use a separate table-management layer when table-level schema evolution and transactional behavior are required; Parquet alone does not provide these guarantees.

## 14.4 Cross-engine compatibility

A Parquet feature being part of the format does not mean every engine supports it equally. Before adopting a less-common encoding, codec, page index, Bloom filter, encryption feature, timestamp representation, or decimal representation, test:

1. Which writer emits it?
2. Which readers must consume it?
3. Which versions are deployed?
4. Does a reader silently ignore the feature or reject the file?
5. Does the feature improve the target workload enough to justify the compatibility surface?

# 15. PyArrow: write, read, inspect, and filter

PyArrow exposes Parquet functionality through `pyarrow.parquet`.

## 15.1 Install

```bash
python -m pip install pyarrow pandas
```

## 15.2 Create sample data and write a Parquet file

```python
import pandas as pd

employees = pd.DataFrame(
    {
        "employee_id": [101, 102, 103, 104, 105, 106],
        "name": ["Asha", "Ravi", "Mei", "Omar", "Lina", "Noah"],
        "department": [
            "Engineering", "Finance", "Engineering",
            "HR", "Finance", "Engineering"
        ],
        "salary": [90000, 75000, 95000, 65000, 82000, 110000],
        "country": ["IN", "IN", "SG", "IN", "SG", "US"],
    }
)

employees.to_parquet(
    "employees.parquet",
    engine="pyarrow",
    compression="zstd",
    index=False,
)
```

## 15.3 Read a complete file

```python
import pandas as pd

employees = pd.read_parquet(
    "employees.parquet",
    engine="pyarrow",
)

print(employees.head())
```

## 15.4 Read selected columns

```python
employees = pd.read_parquet(
    "employees.parquet",
    columns=["employee_id", "salary"],
    engine="pyarrow",
)
```

This demonstrates column projection. Actual I/O savings depend on the file, filesystem, and reader.

## 15.5 Filter with PyArrow

```python
import pyarrow.parquet as pq

filtered = pq.read_table(
    "employees.parquet",
    columns=["employee_id", "salary"],
    filters=[("salary", ">", 90000)],
)

print(filtered.to_pandas())
```

PyArrow can use column statistics to skip row groups when filters are supplied and the metadata is useful.

## 15.6 Inspect file and row-group metadata

```python
import pyarrow.parquet as pq

parquet_file = pq.ParquetFile("employees.parquet")
metadata = parquet_file.metadata

print("Rows:", metadata.num_rows)
print("Columns:", metadata.num_columns)
print("Row groups:", metadata.num_row_groups)
print("Schema:")
print(parquet_file.schema)

for rg_index in range(metadata.num_row_groups):
    row_group = metadata.row_group(rg_index)
    print(f"\nRow group {rg_index}")
    print("Rows:", row_group.num_rows)
    print("Uncompressed bytes:", row_group.total_byte_size)

    for col_index in range(row_group.num_columns):
        column = row_group.column(col_index)
        print("Column:", column.path_in_schema)
        print("  Codec:", column.compression)
        print("  Encodings:", column.encodings)
        print("  Compressed bytes:", column.total_compressed_size)
        print("  Statistics:", column.statistics)
```

## 15.7 Control row-group size

```python
import pyarrow as pa
import pyarrow.parquet as pq

arrow_table = pa.Table.from_pandas(
    employees,
    preserve_index=False,
)

pq.write_table(
    arrow_table,
    "employees_rowgroups.parquet",
    compression="snappy",
    row_group_size=100_000,  # Maximum rows per row group
)
```

`row_group_size` in this API is expressed in **rows**, not bytes. The best row count depends on row width and data distribution.

## 15.8 Use an explicit schema for important pipelines

```python
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema(
    [
        pa.field("employee_id", pa.int64(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("department", pa.string(), nullable=True),
        pa.field("salary", pa.decimal128(18, 2), nullable=True),
    ]
)

# Build an Arrow Table using arrays that conform to this schema.
# Validate values and nullability before writing.
```

An explicit schema reduces the risk of accidental type drift. It does not remove the need for data-quality validation.

# 16. PySpark: write, read, partition, and inspect plans

## 16.1 Write Parquet

```python
df.write \
    .mode("append") \
    .option("compression", "snappy") \
    .parquet("output/employees/")
```

Use `append` only when appending is the intended table operation. `overwrite` can replace data at a path and should be used with care—especially when the path contains multiple partitions or is shared by other jobs.

## 16.2 Read Parquet

```python
df = spark.read.parquet(
    "output/employees/"
)
```

## 16.3 Project columns and filter rows

```python
result = (
    spark.read.parquet("output/employees/")
    .select("employee_id", "salary")
    .where("salary > 90000")
)
```

## 16.4 Set the Parquet compression codec

```python
spark.conf.set(
    "spark.sql.parquet.compression.codec",
    "zstd",
)
```

The effective setting can also be influenced by write options and table properties. Confirm the actual output metadata rather than assuming the session setting won.

## 16.5 Write a partitioned dataset

```python
(
    df.write
    .mode("append")
    .partitionBy("year", "month")
    .parquet("output/orders/")
)
```

Possible layout:

```text
orders/
  year=2026/
    month=01/
      part-....parquet
    month=02/
      part-....parquet
```

Choose partition columns and granularity based on query patterns and volume. Partitioning by a full timestamp or high-cardinality ID is usually a poor default.

## 16.6 `coalesce()` versus `repartition()`

```python
# Reduce partition count with no full shuffle in the common case.
smaller = df.coalesce(20)

# Redistribute data; normally causes a shuffle.
rebalanced = df.repartition(40)
```

These operations influence the number and distribution of output tasks, but they do not guarantee a particular compressed file size. Data skew, row width, writer behavior, and task output all matter.

## 16.7 Inspect the physical plan

```python
result.explain("formatted")
```

Inspect scan details such as:

- The schema being read (`ReadSchema`).
- Partition filters (`PartitionFilters`).
- Pushed filters (`PushedFilters`).
- Files and bytes scanned, where exposed by the plan or runtime metrics.

A displayed pushed filter does not always mean every predicate was fully evaluated in the Parquet reader. The engine may still apply a residual filter after reading data.

# 17. Multi-file Parquet datasets and partitioned layouts

A Parquet **file** has its own schema and footer. A Parquet **dataset** is commonly a collection of Parquet files that a reader discovers and treats as one logical collection. Dataset discovery, directory conventions, and partition metadata sit above the individual file format.

## 17.1 One file versus a dataset

A single Parquet file is self-describing: its footer records the schema and metadata needed to locate its row groups and column chunks.

A multi-file dataset introduces additional concerns:

- Which files belong to the dataset?
- Do all files have compatible schemas?
- Are directory names meaningful partition values?
- How should readers handle missing or additional columns?
- Can files be added or replaced while readers are scanning?

Parquet defines the file representation. It does not, by itself, define a universal directory-discovery protocol or a transaction across a collection of files.

## 17.2 Hive-style partitioned paths

A common directory convention is:

```text
orders/
  year=2025/
    month=12/
      part-000.parquet
  year=2026/
    month=01/
      part-001.parquet
      part-002.parquet
    month=02/
      part-003.parquet
```

The `key=value` directory convention is often called Hive-style partitioning. It is **not encoded as a required directory structure by the Parquet file specification**. Readers such as Spark and PyArrow can interpret this convention when configured to do so.

Partition pruning occurs at the dataset-discovery layer: a reader can avoid opening files in partitions that do not match a partition predicate. This is different from row-group pruning, which uses metadata inside selected Parquet files.

Use partition keys that:

- Match common filter predicates.
- Have a manageable number of distinct values.
- Produce enough data per partition to avoid excessive tiny files.
- Have stable, well-defined types and formatting.

Avoid partitioning by unique IDs, random UUIDs, or highly granular timestamps unless the workload and file volume clearly justify it.

## 17.3 Schema consistency across files

A dataset may contain files written at different times by different writer versions. Common problems include:

- A field is absent from older files but present in newer ones.
- A field changes physical or logical type.
- Decimal precision or scale changes.
- Timestamp units or semantics differ.
- Field names differ only by case.
- Nested list or map schemas are not represented consistently.

Do not assume that a reader can safely merge every schema variation. Define a dataset schema contract, validate new files, and test schema-unification behavior in the actual reader.

## 17.4 Read a dataset with PyArrow

```python
import pyarrow as pa
import pyarrow.dataset as ds

# The directory contains Hive-style paths such as year=2026/month=01.
dataset = ds.dataset(
    "orders/",
    format="parquet",
    partitioning="hive",
)

# Project two data columns and filter using a partition field.
table = dataset.to_table(
    columns=["customer_id", "amount"],
    filter=(ds.field("year") == 2026)
        & (ds.field("month") == 1),
)

print(table.schema)
print(table.num_rows)
```

This example illustrates dataset discovery, projection, and partition filtering. Exact scan behavior depends on the PyArrow version, partition schema, file contents, and filter expression.

## 17.5 Compaction is a Parquet rewrite

Compaction reads records from multiple smaller Parquet files and writes them into fewer files, usually with newly formed row groups and pages.

A compaction process should:

1. Read the source files using a validated schema.
2. Preserve logical values, null behavior, decimal semantics, and timestamp meaning.
3. Choose output file and row-group sizing based on measured workloads.
4. Write to a new location or a safely isolated staging area.
5. Validate row counts, schema, and representative values.
6. Publish the new file set using the dataset's external coordination mechanism.
7. Retire old files only after readers can no longer depend on them.

Compaction is not a byte-level concatenation of Parquet files. A correct rewrite reads and re-encodes data into new Parquet structures.

## 17.6 Boundary: what a Parquet dataset does not guarantee

A directory of Parquet files does not automatically provide atomic multi-file commits, snapshot isolation, concurrent-writer coordination, or time travel. These are table-management concerns implemented by systems layered above Parquet. Keep that distinction clear: Parquet describes files; a dataset reader discovers files; a table-management layer can coordinate changes to a logical table.

# 18. Parquet interoperability and reader/writer compatibility

Parquet has a format specification, but real-world behavior also depends on the writer and reader implementations and their versions. Two systems can both support Parquet while differing in support for optional features, logical types, timestamp conventions, or newer metadata.

## 18.1 Specification versus implementation

The Parquet specification defines how structures and values are represented. A particular library may implement only a subset of optional features or may expose different configuration controls.

For production use, distinguish:

- **Format-valid:** the file follows the Parquet specification.
- **Writer-supported:** the writer can produce the feature or representation.
- **Reader-supported:** the target reader can interpret it correctly.
- **Reader-optimized:** the reader can use the feature for efficient execution, not merely decode it.

For example, a file can contain page indexes or Bloom filters, but a reader may ignore them and still read the data correctly.

## 18.2 Compatibility-sensitive areas

Validate these areas whenever multiple engines or library versions exchange Parquet files:

| Area | What to verify |
|---|---|
| Logical types | STRING, DATE, DECIMAL, UUID, and integer annotations are interpreted consistently. |
| Timestamps | Physical representation, unit, UTC-adjustment semantics, and timezone assumptions agree. |
| Decimal | Precision, scale, and physical representation are supported by every reader. |
| Compression | Every consumer supports the codec used by the writer. |
| Data-page versions | Readers support the page versions emitted by the writer. |
| Nested data | LIST and MAP structures have compatible schema representations. |
| Field IDs | Readers preserve and use field identity as intended when field IDs are present. |
| Optional metadata | Statistics, page indexes, Bloom filters, encryption, and checksums are supported where relied upon. |
| Legacy timestamps | INT96 handling is consistent when older files still use it. |

Do not infer compatibility solely from a `.parquet` filename. Test the actual files produced by the deployed writer.

## 18.3 Build a writer-reader compatibility matrix

For each production writer, record:

- Library or engine and version.
- Compression codec and level, if applicable.
- Data-page version and encodings.
- Timestamp and decimal conventions.
- Nested schema patterns.
- Optional metadata features enabled.

For each production reader, record whether it can decode each feature, whether it interprets values identically, and whether it can use the feature for pruning.

A practical test corpus should include:

1. Nullable primitive columns.
2. Decimal values at precision and scale boundaries.
3. Timestamps before and after daylight-saving transitions where relevant.
4. Empty, null, and populated lists and maps.
5. Dictionary-encoded and plain-encoded columns.
6. Each approved compression codec.
7. Files with multiple row groups and nontrivial statistics.
8. Files with optional page indexes or Bloom filters, if production relies on them.

## 18.4 Safe interoperability practices

- Prefer standard logical types and avoid legacy representations for new data unless required for compatibility.
- Use explicit schemas for critical datasets.
- Standardize timestamp meaning, not just timestamp data type.
- Test decimal precision and scale at boundary values.
- Enable advanced metadata only after confirming the writer and reader support matrix.
- Keep representative Parquet fixtures in automated compatibility tests.
- Re-test when upgrading Spark, PyArrow, or another Parquet implementation.

# 19. Production troubleshooting playbook

## 19.1 Symptom: queries are slow despite using Parquet

Investigate in this order:

1. **Scan volume:** How many bytes are scanned? Are too many partitions or files selected?
2. **Projection:** Is the query reading all columns when it needs only a few?
3. **File layout:** Are there thousands of tiny files or a few oversized files?
4. **Pruning:** Are partition filters and Parquet filters being applied?
5. **Statistics:** Are min/max ranges selective or do most row groups overlap the predicate range?
6. **Data distribution:** Is the data randomly distributed or clustered by common filter columns?
7. **Compute:** Is time spent in decompression, shuffle, join skew, spill, or planning rather than storage reads?
8. **Compatibility:** Does the reader support the codecs and features used by the writer efficiently?

## 19.2 Symptom: too many small files

**Likely causes:** too many Spark output partitions, frequent micro-batches, high-cardinality partition columns, or many small upstream inputs.

**Remedies:** compact files, tune output partition counts, reconsider partition granularity, and establish a periodic maintenance job. Ensure compaction is safe for concurrent writers and table commits.

## 19.3 Symptom: partition pruning does not happen

**Check:**

- Is the filter on a declared partition column?
- Does the table/catalog know the partition values?
- Is the predicate written in a form the engine can reason about?
- Is a cast or function around the partition key preventing pruning?
- Does the plan list the expected partition filters?

Prefer direct and type-correct predicates on partition columns where possible.

## 19.4 Symptom: row-group pruning is ineffective

**Likely causes:** broad or overlapping min/max ranges, randomly distributed values, missing or unusable statistics, unsupported predicate pushdown, or reader limitations.

**Remedies:** inspect row-group statistics, cluster or sort by relevant columns when justified, test supported filter predicates, and validate reader behavior.

## 19.5 Symptom: schema mismatch or conversion exceptions

**Likely causes:** type drift, inconsistent decimal precision/scale, timestamp differences, incompatible nullability assumptions, or mixed schemas across files.

**Remedies:** enforce an explicit schema, validate data before writing, quarantine incompatible files, define migration rules, and test the complete reader matrix.

## 19.6 Symptom: unexpectedly large files

**Check:**

- Whether too few output tasks are writing.
- Whether partitions are uneven or skewed.
- Whether a single output partition has accumulated a large volume.
- Whether compression or dictionary behavior changed.

Increase parallelism only if it improves throughput without creating too many small files.

## 19.7 Symptom: reader cannot open a Parquet file

Check:

- File completeness and footer/trailer validity.
- Codec support.
- Logical type and timestamp compatibility.
- Writer version and feature support.
- Whether the object was partially overwritten or truncated.
- Whether encryption or checksums are enabled and supported by the reader.

# 20. Benchmarking and design checklist

## 20.1 Benchmark plan

Use representative production-like data. Keep the row count, schema, data distribution, and query workload consistent when comparing configurations.

Compare a small matrix of:

- Compression: Snappy versus ZSTD; optionally GZIP.
- Row-group sizing: smaller, medium, and larger settings supported by the writer.
- Data order: random versus clustered/sorted on a common filter key.
- File sizing: small-file output versus compacted output.
- Projection: full schema versus a narrow subset of columns.
- Predicate selectivity: broad versus selective filters.

Capture:

| Metric | Why it matters |
|---|---|
| Compressed bytes | Storage and scan cost |
| Write time | Pipeline throughput |
| Read time | Consumer performance |
| CPU time | Encode/decode cost |
| Bytes scanned | Effectiveness of pruning and projection |
| Number of files | Listing, planning, and scheduling overhead |
| Number of row groups | Pruning granularity and metadata overhead |
| Peak memory / spill | Resource pressure |
| Query planning time | Small-file and metadata overhead |

## 20.2 Design checklist

### Data contract
- [ ] Are column names, types, nullability, decimals, and timestamps defined?
- [ ] Is schema evolution governed and tested?
- [ ] Are all target readers compatible with the chosen features?

### File and row-group layout
- [ ] Are file sizes measured rather than guessed?
- [ ] Are row-group sizes tuned for the workload?
- [ ] Is the small-file problem monitored?
- [ ] Is output parallelism appropriate?

### Query performance
- [ ] Are partition columns aligned with common filters?
- [ ] Are queries projecting only required columns?
- [ ] Are row-group statistics useful?
- [ ] Would sorting or clustering improve selectivity?
- [ ] Are page indexes or Bloom filters supported and worthwhile?

### Production operations
- [ ] Are writes idempotent or protected by table-level commit semantics?
- [ ] Are late data and reprocessing handled?
- [ ] Is compaction safe with concurrent readers and writers?
- [ ] Are encryption, IAM, retention, and metadata exposure addressed?
- [ ] Are bytes scanned, file counts, and query latency monitored?

# 21. Senior Data Engineer interview questions

## Fundamentals

**1. What is Apache Parquet?**  
A column-oriented binary file format designed for efficient storage and analytical reads. It supports nested data, encodings, compression, and metadata useful for projection and data skipping.

**2. Why is Parquet generally effective for analytics?**  
Analytical queries often read a subset of columns across many rows. Parquet stores data by column chunk, so readers can avoid unrelated columns and can use metadata to skip data that cannot match filters.

**3. Explain file, row group, column chunk, and page.**  
A file contains row groups. Each row group contains one column chunk per column. A column chunk contains pages. Pages are the encoding/compression units.

**4. Is a row group the same as an HDFS block or Spark partition?**  
No. A row group is a logical Parquet structure. Storage blocks, Spark partitions, and execution tasks are different concepts and may have different boundaries.

**5. Why is the footer important?**  
It contains schema and metadata describing row groups and column chunks, allowing readers to locate selected data without scanning the whole file sequentially.

## Encoding and compression

**6. What is the difference between encoding and compression?**  
Encoding changes the representation of values; compression reduces the size of encoded page data. Dictionary and delta are encodings; Snappy and ZSTD are codecs.

**7. When is dictionary encoding useful?**  
When a column repeats a relatively small set of values. It can be inefficient for near-unique values, and writers may fall back to plain encoding.

**8. What is delta encoding?**  
It stores integer differences rather than every full value. Parquet's delta-binary-packed encoding supports `INT32` and `INT64`.

**9. Is Snappy always better than GZIP or ZSTD?**  
No. Snappy often prioritizes speed, while GZIP or ZSTD may provide better compression at different CPU costs. Benchmark using the actual workload and compatible readers.

## Read optimization

**10. What is column pruning?**  
Reading only the columns required by a query.

**11. What is predicate pushdown?**  
Passing filters to the data source/reader so it can apply them early and potentially skip irrelevant data. The engine may still apply residual filters after reading.

**12. What is the difference between partition pruning and row-group pruning?**  
Partition pruning skips directories or partition values at the dataset/table level. Row-group pruning skips row groups within selected Parquet files using metadata such as min/max statistics.

**13. How do min/max statistics help?**  
If a predicate's possible values do not overlap a row group's min/max range, the row group cannot match and can be skipped.

**14. What is a Bloom filter, and when is it useful?**  
It is a probabilistic membership structure that can prove a value is absent or say it may be present. It can help with equality predicates on high-cardinality columns when supported by writer and reader.

**15. What is a page index?**  
Optional metadata containing page-level indexes, including value bounds and offsets, that can help readers skip pages within a column chunk.

## Data layout and operations

**16. How would you choose file size and row-group size?**  
Use workload-driven benchmarks. Account for file-listing overhead, scan parallelism, pruning granularity, compression, writer memory, and query-engine behavior. Do not treat a generic target as a format requirement.

**17. How would you reduce small files in Spark?**  
Review upstream partition counts, shuffle behavior, and partition cardinality; compact existing data; tune output tasks; and avoid creating one directory per high-cardinality value.

**18. Why can sorting improve query performance?**  
It can tighten min/max ranges within row groups, allowing more row groups to be skipped for range predicates. Sorting can require expensive shuffles, so measure the trade-off.

**19. How do you handle schema evolution?**  
Use an explicit contract, validate incoming schemas, make nullability and logical types deliberate, test mixed-version files across readers, and use a separate table-management layer when table-level schema evolution and transactions are required.

**20. Why are timestamp fields risky across engines?**  
Timestamp units and timezone semantics can differ. A pipeline must specify whether values represent instants or local wall-clock times, choose a supported unit, and test writer-reader compatibility.

## Scenario-based questions

**21. A query engine scans far more Parquet data than the result size suggests. What do you inspect?**  
Selected columns, files discovered, partition filters, row-group statistics, data clustering, pushed filters, bytes read, and whether the time is spent decoding Parquet or in later query operators.

**22. A filter on `customer_id` skips almost no row groups. Why?**  
If customer IDs are randomly distributed, min/max ranges in many row groups may overlap widely. Sorting or clustering may help; Bloom filters may help for equality predicates if writer and reader support them.

**23. A pipeline has one output file per Spark task, but many files are tiny. What would you change?**  
Check the number of upstream and post-shuffle partitions, skew, input volume per task, and partition columns. Compact output and tune task count based on measured file-size distribution.

**24. A new writer emits a codec that an older consumer cannot read. How should the platform prevent this?**  
Maintain a compatibility matrix, choose a platform-wide supported codec/feature profile, test representative files in CI with all critical readers, and roll out changes deliberately.

**25. Why does a directory of Parquet files not guarantee an atomic table update?**  
Each Parquet file is an independent file with its own footer. The Parquet format does not define a transaction spanning several files or a snapshot of a directory. Atomic publication and concurrent-writer coordination require an external table-management or commit mechanism.

# 22. Hands-on labs

## Lab 1 — Inspect a Parquet file

1. Write a DataFrame to Parquet.
2. Inspect schema, row count, row-group count, compression codec, encodings, and statistics.
3. Confirm the number of rows in each row group.
4. Compare compressed and uncompressed sizes.

**Outcome:** Explain what the footer tells a reader.

## Lab 2 — Compare compression codecs

Write the same representative dataset using Snappy, GZIP, and ZSTD.

Record:

- File size.
- Write duration.
- Read duration.
- CPU consumption.

**Outcome:** Make a codec recommendation based on evidence rather than habit.

## Lab 3 — Demonstrate column pruning

Create a wide table with many columns. Run one query that reads all columns and another that reads only two columns.

Compare physical plans, bytes read, and execution time.

**Outcome:** Explain why columnar storage helps narrow analytical queries.

## Lab 4 — Demonstrate row-group pruning

Create two datasets:

- One randomly distributed by `event_date`.
- One sorted by `event_date` before writing.

Write multiple row groups, inspect min/max statistics, and run the same date-range filter.

**Outcome:** Explain how data ordering affects statistics selectivity.

## Lab 5 — Demonstrate the small-file problem

Write a dataset as many tiny files, query it, compact it, and query it again.

Compare planning time, file count, task count, and query latency.

**Outcome:** Explain why file layout matters independently of the file format.

## Lab 6 — Build and inspect a multi-file Parquet dataset

1. Create a representative Arrow table with date, integer, decimal, string, and nullable fields.
2. Write multiple Parquet files into a directory, using a date-derived Hive-style partition layout.
3. Read the directory as a PyArrow dataset.
4. Project only a subset of columns and filter on a partition field.
5. Inspect individual files, schemas, row groups, encodings, compression codecs, and statistics.
6. Introduce a controlled schema variation and observe how the reader handles it.
7. Compact the files by rewriting them with a different file and row-group sizing strategy.
8. Validate row counts, values, and schema before and after compaction.

**Outcome:** Understand the distinction between Parquet file internals and the conventions used to organize multiple Parquet files.

# 23. One-page revision sheet

| Term | Remember |
|---|---|
| Parquet | Columnar file format for analytical data |
| `PAR1` | Four-byte file magic value at beginning and end |
| Footer | Schema and file metadata, written after data |
| Row group | Logical horizontal partition of rows |
| Column chunk | One column within one row group; pages are contiguous |
| Page | Unit of encoding and compression |
| Dictionary | Distinct values plus encoded IDs; may fall back to plain |
| RLE/bit-packing | Compact representation for runs and small integer values |
| Delta | Encode differences between integer values |
| Snappy | Often fast, moderate compression |
| ZSTD | Configurable compression; benchmark CPU and size |
| Statistics | Metadata such as min, max, and null count |
| Partition pruning | Skip partition directories/values |
| Column pruning | Avoid unneeded column chunks |
| Row-group pruning | Skip row groups using metadata |
| Page index | Optional page-level navigation metadata |
| Bloom filter | Can prove absence; may report false positives |
| Definition level | Represents optionality / how much of a nested path is present |
| Repetition level | Represents repeated nested values |
| Dataset | A collection of Parquet files read as one logical collection; discovery and commit behavior are external to an individual file |

## The read-path mental model

```text
Query
  ↓
Discover table files and partitions
  ↓
Partition pruning
  ↓
Read Parquet footer / schema / metadata
  ↓
Column pruning
  ↓
Row-group statistics, dictionaries, Bloom filters
  ↓
Page index (when written and supported)
  ↓
Read required column chunks / pages
  ↓
Decompress and decode
  ↓
Apply remaining filters and query operators
  ↓
Return result
```

**Suggested study order:** Sections 1–7 for the fundamentals; 8–11 for internals and query optimization; 12–17 for production engineering; and 19–22 for troubleshooting and interview practice.
