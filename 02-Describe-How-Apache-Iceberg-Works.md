## 1. What Apache Iceberg is

Apache Iceberg is an **open table format** for managing large analytic tables stored as files. It defines how table state is represented, how data files are tracked, how schema and partition metadata are interpreted, and how table changes become visible atomically.

> An Iceberg table is not defined by listing every file found under a storage directory. Its authoritative state is defined by Iceberg metadata and the snapshot referenced by that metadata.

A table can contain many data files, but Iceberg records exactly which files belong to each valid snapshot. A writer creates new files and metadata, then commits a new table state. A reader uses one consistent snapshot rather than reconstructing the table by listing storage paths.

### 1.1 What Iceberg manages

Iceberg manages:

- **Table identity and metadata:** table schema, partition specifications, sort orders, table properties, snapshots, and references.
- **Data-file membership:** which files are live in a particular snapshot.
- **Evolution:** safe changes to schemas and partition specifications over time.
- **Version history:** snapshots, time travel, rollback, branches, and tags.
- **Commit correctness:** atomic publication of a new table metadata state and optimistic concurrency handling.
- **File-level planning metadata:** partition values, file sizes, record counts, and column-level metrics used to eliminate unnecessary files.

### 1.2 What Iceberg does not do by itself

Iceberg is not, by itself, a query engine, a scheduler, or a complete data pipeline. A compute engine executes reads and writes. A catalog resolves a table identifier and coordinates the current metadata location. A storage system holds the data and metadata files. Iceberg defines the table-format contract among these pieces.
Iceberg provides table semantics and metadata management; the engine still has to implement the relevant Iceberg read/write behavior.

### 1.3 Why Iceberg exists

Large file-based tables need reliable answers to questions such as:

- Which files belong to the table right now?
- What exact state should a query read while a write is in progress?
- How can a column be renamed without confusing old files with new columns?
- How can the physical partition layout change without rewriting all historical data?
- How can two writers attempt changes at the same time without exposing a partially committed table?
- When is a file safe to delete?

Iceberg answers these questions through metadata, snapshots, immutable data files, and atomic metadata commits. The official overview describes snapshot-based reads, schema and partition evolution, time travel, and optimistic concurrent writes as core capabilities.

---

## 2. Core terminology

| Term | Meaning | Interview significance |
|---|---|---|
| Table | A logical collection of rows whose state is defined by Iceberg metadata | Do not equate it with a directory listing |
| Catalog | Resolves table identifiers and stores or coordinates the current metadata pointer | It is the lookup and commit-coordination layer |
| Table metadata file | A JSON metadata document describing the table's schemas, specs, properties, snapshots, and references | It is the entry point into the metadata tree |
| Snapshot | A consistent state of the table at a point in its history | Basis for isolation, time travel, and rollback |
| Manifest list | For established v1–v3 layouts, a file that lists the manifests belonging to a snapshot and records useful manifest-level statistics | Helps planning avoid reading every manifest |
| Manifest | Metadata file containing entries for a subset of data or delete files | Tracks file paths, partition tuples, status, and metrics |
| Data file | A file containing table rows | Common data formats include Parquet, Avro, and ORC |
| Delete file | Metadata/data file encoding rows that should be excluded from the visible table state | Enables row-level deletes without requiring immediate full-file rewrites |
| Partition spec | The set of source fields and transforms that determines partition values | Can evolve; old files retain the spec under which they were written |
| Field ID | Stable numeric identity assigned to a schema field | Makes renames and reordering safe |
| Sequence number | Monotonic metadata value used to order changes and determine delete applicability | Important for understanding row-level deletes |
| Snapshot reference | A named branch or tag pointing to a snapshot | Supports isolated work and historical retention policies |
| Orphan file | A file under a table location that is not referenced by valid table metadata | May result from failed or abandoned writes; requires safe cleanup |

## 2.1 A Memory Aid

- **Catalog:** Where do I find the table's current metadata?
- **Metadata JSON:** What is the table definition and which snapshot is current?
- **Snapshot:** What is the complete table state at this point in history?
- **Manifest list:** Which manifests describe that snapshot?
- **Manifest:** Which files are tracked, and what are their partition values and metrics?
- **Data/delete files:** Where are the rows and row-level delete information?

---

## 3. Iceberg table architecture

<p align="center">
  <img width="500" height="550" alt="image" src="https://github.com/user-attachments/assets/10423665-08fd-4d17-b133-626cf080b492" />
</p>

### 3.1 The metadata tree

For the established v1–v3 layout, the conceptual structure is:

```text
Catalog
  |
  | resolves table identifier to current metadata location
  v
Table metadata JSON
  |-- current schema and schema history
  |-- current partition spec and spec history
  |-- sort orders
  |-- table properties
  |-- snapshot log and snapshot references
  |
  v
Current snapshot
  |
  v
Manifest list (v2/v3; v1 has legacy manifest-reference behavior)
  |-- manifest A + partition/file summaries
  |-- manifest B + partition/file summaries
  |-- manifest C + partition/file summaries
       |
       v
     Manifest files
       |-- live data-file entries
       |-- delete-file entries where applicable
       |-- partition tuples and file metrics
       |
       v
     Data files and delete files in storage
```

The diagram is logical. Exact metadata fields and file representations depend on the table-format version. In particular, the current specification describes a different root-manifest structure for the still-developing v4 format; do not silently apply that structure to v1–v3 tables.

### 3.2 Table metadata JSON

The table metadata file is the table's metadata entry point. It includes information such as:

- Table format version.
- Table UUID and table location.
- Current schema ID and the list of known schemas.
- Current partition-spec ID and known partition specs.
- Sort orders.
- Table properties.
- Snapshot history and the current snapshot ID.
- Snapshot references such as branches and tags.
- A log of previous metadata files, subject to metadata-retention settings.

A metadata-file update does not necessarily mean the table's row data changed. For example, a schema change or property change can update table metadata without rewriting data files. A data-changing commit normally adds a new snapshot.

### 3.3 Snapshots

A snapshot represents the complete visible table state at a particular point in time. It has a unique snapshot ID and, for v2/v3, a sequence number. It can also contain a parent snapshot ID, a timestamp, a summary, and the manifest-list location.

A critical detail: **a snapshot is not merely a list of files newly written by one job**. It resolves to the complete set of live files for the table at that point. Iceberg reuses manifests from previous snapshots where possible, so a new snapshot does not require rewriting all historical metadata.

For example:

```text
Snapshot S1: data files A, B
Snapshot S2: data files A, B, C    (append C)
Snapshot S3: data files A, C       (remove B, perhaps through overwrite/compaction)
```

S2 still describes A, B, and C even after S3 becomes current. B can remain necessary for a query time-travelling to S2. It is only eligible for physical deletion once no retained snapshot or reference requires it.

### 3.4 Manifest list

In v2/v3, the manifest list is associated with a snapshot and identifies the manifests that make up that snapshot. It stores information about manifests, including partition summaries and file counts. This lets the reader skip manifests that cannot contain relevant files.

Think of the manifest list as an index over manifest files, not as a row-level data file.

### 3.5 Manifest files

A manifest is an immutable metadata file containing entries for a subset of a table's data files or delete files. Entries typically record:

- File path and file format.
- File size and record count.
- Partition tuple and partition-spec ID.
- File entry status, such as added, existing, or deleted.
- Sequence-number metadata.
- Column-level metrics such as null counts, value counts, and lower/upper bounds, where collected.
- Sort-order information where applicable.

A manifest can track files from many partitions; a manifest is not itself a physical partition directory. In v1–v3, all entries in a manifest use the same partition spec, but the manifest is not restricted to one partition value.

Manifest reuse is important. If a commit appends new files, Iceberg can often add a new manifest while reusing manifests that still describe unchanged files.

### 3.6 Data files

Data files contain the actual rows. Iceberg commonly uses Parquet, Avro, or ORC as its data-file formats. The Iceberg table metadata is responsible for interpreting those files as one logical table, including field IDs and partition information.

Do not assume that the folder names or path layout define the table's logical schema or partition semantics. Iceberg tracks files explicitly in metadata.

### 3.7 Metadata-file layout versus data-file layout

A useful interview distinction:

- A **data file** contains records.
- A **manifest** contains metadata about data/delete files.
- A **manifest list** contains metadata about manifests.
- A **table metadata JSON file** describes the table and points to snapshots.

This hierarchy prevents a common interview error: calling a manifest a data file or saying that a snapshot contains the rows directly.

---

# 5. How a read works

A read is planned from a consistent table snapshot.

## 5.1 Read path, step by step

1. The reader resolves the table identifier through the configured catalog, or loads a table by a supported direct location mechanism.
2. It obtains the current table metadata and chooses the current snapshot, or a requested historical snapshot/reference.
3. It reads the snapshot's manifest list (for v2/v3) to identify candidate manifests.
4. It uses partition summaries and other manifest-level information to skip irrelevant manifests.
5. It examines candidate manifest entries and uses partition values and file-level metrics to skip files that cannot match the query predicate.
6. It plans reads for the remaining data files.
7. Where row-level delete files apply, it applies the relevant delete information so deleted rows are not returned.
8. It reads the selected files and returns the result.

## 5.2 Why readers see a consistent snapshot

The reader uses the snapshot that was current when it loaded table metadata. A concurrent commit can publish a newer metadata location, but the already-running reader continues to use the snapshot it selected. It does not see a mixture of old and new file membership merely because a writer is running.

A later query can refresh metadata and select the newer snapshot.

## 5.3 Predicate and file pruning

Iceberg uses metadata to eliminate files before reading their rows. For example, suppose a query asks for:

```sql
SELECT customer_id, amount
FROM sales
WHERE order_date >= DATE '2026-10-01'
  AND order_date <  DATE '2026-11-01'
  AND customer_id = 42;
```

Iceberg may use:

- The partition transform on `order_date` to eliminate irrelevant partition values.
- Manifest summaries to avoid opening manifests whose files cannot match.
- File-level lower and upper bounds, null counts, and value counts to eliminate additional files.
- The reader's data-file capabilities for column projection and predicate pushdown after files have been selected.

Pruning is conservative. Metadata must not incorrectly exclude a file that could contain matching rows. Missing or truncated metrics can reduce pruning effectiveness, but should not be treated as proof that a file has no matching rows.

## 5.4 Query-planning cost

A table can have a small amount of row data but a large amount of metadata if it has millions of tiny files or too many manifests. Query planning may then spend significant time opening metadata and generating file tasks before actual data scanning begins.

This is why data-file compaction and manifest rewriting are operational tasks, not merely cosmetic cleanup.

---

# 6. How a write and commit work

This is one of the highest-value interview topics.

## 6.1 High-level write path

A typical Iceberg write follows this sequence:

1. **Load table state.** The writer reads the current table metadata and snapshot.
2. **Plan work.** It determines the operation and the files or rows involved.
3. **Write new files.** Tasks write data files and, where the operation requires them, delete files. Files are written to unique paths; they are not made visible merely by existing in storage.
4. **Build metadata.** The writer creates manifest files and the metadata needed for a new snapshot.
5. **Validate assumptions.** Before committing, the writer checks that the operation is still valid against the current table state. The checks depend on the operation.
6. **Publish atomically.** The writer asks the catalog/table operations layer to replace the current metadata location with the newly written metadata location using an atomic commit mechanism.
7. **Handle conflicts.** If another writer committed first, the writer refreshes, validates its assumptions again, and retries when safe. Otherwise, it reports a conflict.

The most important interview sentence is: **writing files is not the commit; publishing the new table metadata is the commit.**

## 6.2 Why write files before committing metadata?

New files can be created without changing what readers see. Until the metadata commit succeeds, those files are not part of the table's committed snapshot. This separates file creation from table-state publication.

If a job fails before the commit, it can leave unreferenced files. These are potential orphan files and can be cleaned up later after a sufficiently conservative safety interval.

## 6.3 Atomic metadata commit

The commit changes which table metadata file is current. The catalog or table-operations implementation must provide the needed atomic update, commonly through a compare-and-swap/check-and-put style operation.

A successful commit publishes one complete new table state. Readers do not observe half of a new manifest tree or a partial list of files.

The Iceberg reliability documentation describes this atomic metadata replacement as the basis for reliable reads and serializable table updates. The exact mechanism depends on the catalog and storage implementation.

## 6.4 Optimistic concurrency control

Iceberg uses optimistic concurrency for concurrent writers:

- Writers do not hold a global table lock for the whole data-writing job.
- Each writer prepares a proposed new state based on the state it read.
- A writer attempts to atomically replace the current metadata location.
- If another writer has committed first, the attempt fails or requires retry.
- The writer refreshes the table and validates whether its assumptions still hold.
- If the operation remains safe, the writer can retry the commit; otherwise, it must fail rather than overwrite incompatible changes.

### Example: concurrent append

Writer A and Writer B both read snapshot S10. A writes file A1; B writes file B1. A commits S11. B's original commit is now based on stale metadata. B refreshes, checks whether its append can still be applied, and may commit a snapshot that includes both A1 and B1.

### Example: compaction conflicts with delete

A compaction job reads files F1 and F2 and creates replacement file F3. At the same time, another writer deletes or replaces F1. The compaction must validate that F1 and F2 are still present and eligible for replacement. If not, blindly removing F1 and F2 could lose a concurrent change, so the compaction must not commit as originally planned.

## 6.5 What concurrency control does not mean

- It does not mean every pair of operations can always be merged.
- It does not mean there are no commit retries or conflicts.
- It does not make a multi-table business workflow one atomic transaction automatically.
- It does not guarantee that every engine-specific command implements identical validation behavior.

A senior answer should mention both the atomic commit and the operation-specific validation rules.

---

## 6. Snapshots, Time travel, and Rollback

### 6.1 Snapshot versus metadata version

A metadata JSON file version and a table snapshot are related but different concepts:

- A new metadata file records a new table metadata state.
- A snapshot records a particular table data state.
- Some metadata-only changes do not create a new data snapshot.
- A data-changing commit normally creates a new snapshot.

Do not use “snapshot” and “metadata JSON version” as synonyms.

### 6.2 Time travel

Time travel means reading a table as it existed at a retained snapshot or timestamp. It is useful for:

- Reproducing a historical result.
- Auditing what a query would have seen.
- Comparing data before and after a write.
- Investigating a faulty ingestion or transformation.
- Supporting controlled recovery procedures.

Spark SQL example (syntax is integration-specific):

```sql
-- Read a specific retained snapshot
SELECT *
FROM prod.analytics.events VERSION AS OF 1234567890123;

-- Read the snapshot current at a timestamp
SELECT *
FROM prod.analytics.events TIMESTAMP AS OF '2026-10-31 23:59:59';
```

Time travel is only possible while the relevant snapshot and referenced files have not been expired or otherwise removed.

### 6.3 Rollback

Rollback changes the table's current snapshot reference to a prior valid snapshot. It does not require copying every row back into newly written files. The prior snapshot already describes the data-file set that should become current.

Spark procedure example:

```sql
CALL prod_catalog.system.rollback_to_snapshot(
  'analytics.events',
  1234567890123
);
```

A rollback is a table-state operation, not a substitute for investigating the cause of a bad write. Confirm the selected snapshot, branch/reference state, and downstream dependencies before performing it.

### 6.4 Snapshot retention

Snapshots accumulate as the table changes. Retention policy determines how far back time travel and rollback remain possible and when data files that are no longer needed can be physically removed.

Important consequences:

- Expiring a snapshot removes that historical query point from the retained history.
- Files still referenced by retained snapshots must remain available.
- A branch or tag can keep a snapshot reachable even when it is old.
- Longer retention increases the amount of historical metadata and potentially retained data storage.

A retention policy is therefore both a storage-cost policy and a recovery/audit policy.

---

# 8. Schema evolution and field IDs

Schema evolution changes the logical table schema without requiring all existing data files to be rewritten.

## 8.1 Supported schema changes

Iceberg supports, subject to type and implementation rules:

- Add a column or nested field.
- Drop a column or nested field.
- Rename a column or nested field.
- Widen a supported type.
- Reorder columns or nested fields.

Examples of common safe promotions include `int` to `long`, `float` to `double`, and increasing decimal precision without changing scale. The exact allowed promotions depend on the table-format version and field usage; a partition-source field has additional restrictions because a type change must not silently change the meaning of its partition transform.

## 8.2 Why field IDs matter

Iceberg assigns stable numeric field IDs to schema fields. Readers project columns using these IDs rather than relying only on column names or ordinal positions.

Imagine a table was originally written with:

```text
field ID 1 -> customer_id
field ID 2 -> customer_name
field ID 3 -> email
```

Later, `customer_name` is renamed to `full_name`. The field retains its ID. An old data file may still contain the historical physical name, but Iceberg can map it to the current logical field because the ID is stable.

This protects against a dangerous situation in name/position-based interpretation: a dropped column's old position being reused by a new column and historical values appearing under the wrong name.

## 8.3 What happens to old files after a schema change?

Suppose a new `country` column is added:

- Old data files do not suddenly gain a physical `country` column.
- The Iceberg schema includes the new field.
- For old files that do not contain the field, the reader resolves the value according to the Iceberg schema/default rules, commonly `NULL` when no applicable default exists.
- New files can physically contain the new field.

This is why a metadata-only schema change can be safe without rewriting historical data.

## 8.4 Field IDs and nested structures

Field IDs apply to nested schema fields too. A senior engineer should not treat nested-field changes as a special case that can ignore field identity. Add, drop, rename, reorder, and type-promotion rules need to preserve the identity and meaning of nested fields.

## 8.5 Defaults

Format v3 adds support for column default values. Distinguish a default value recorded in table schema metadata from a value physically present in every historical file. Readers apply the format's schema/default interpretation; writers and readers must support the relevant versioned behavior.

## 8.6 Common schema-evolution interview question

**Question:** “A column is renamed. How can Iceberg read files written before the rename?”

**Answer:** Iceberg uses stable field IDs. The renamed logical field keeps the same ID, so readers map the historical physical field to the current schema field. The rename is primarily a metadata change and does not require rewriting all existing files.

---

## 8. Hidden Partitioning & Partition Evolution

Partitioning groups rows so that queries can avoid scanning files that cannot contain matching records. Iceberg calls its approach hidden partitioning because users filter on source columns; they do not need to add or maintain derived partition columns in their queries.

### 8.1 Partition transforms

Common transforms include:

| Transform | Purpose | Example |
|---|---|---|
| `identity(col)` | Use the source value directly | Partition by `region` |
| `year(ts)` | Derive the year from a date/timestamp | `year(event_ts)` |
| `month(ts)` | Derive the month | `month(event_ts)` |
| `day(ts)` / `date(ts)` | Derive the date | `day(event_ts)` |
| `hour(ts)` | Derive the hour | `hour(event_ts)` |
| `bucket(N, col)` | Hash a value into N buckets | `bucket(32, customer_id)` |
| `truncate(W, col)` | Truncate values to a width | `truncate(10, postal_code)` or a numeric bin |
| `void` | Retain a dropped partition field in a compatibility-oriented spec transition | Usually not a transform to choose for a new design |

The exact SQL spelling can vary by integration. Spark DDL accepts transform expressions such as `days(ts)` and `bucket(16, id)`; check the target engine's Iceberg documentation.

### 8.2 Example of hidden partitioning

Spark SQL example:

```sql
CREATE TABLE prod.analytics.events (
  event_id     BIGINT,
  event_ts     TIMESTAMP,
  customer_id  BIGINT,
  event_type   STRING,
  amount       DECIMAL(18, 2)
)
USING iceberg
PARTITIONED BY (days(event_ts), bucket(32, customer_id))
TBLPROPERTIES ('format-version' = '2');
```

A query can filter on `event_ts` and `customer_id` directly:

```sql
SELECT event_type, SUM(amount)
FROM prod.analytics.events
WHERE event_ts >= TIMESTAMP '2026-10-01 00:00:00'
  AND event_ts <  TIMESTAMP '2026-11-01 00:00:00'
  AND customer_id = 42
GROUP BY event_type;
```

Iceberg translates source-column predicates into constraints on partition transforms where possible. The query does not need to expose or calculate the hidden partition columns itself.

### 8.3 How to choose transforms

Choose transforms based on actual query predicates and data distribution:

- Use a time transform when queries commonly restrict time ranges.
- Use bucket partitioning for high-cardinality keys when equality predicates are common and spreading data across buckets is useful.
- Use identity partitioning for low/controlled-cardinality categories when it supports the query workload.
- Use truncate partitioning when grouping nearby values is useful for the workload.
- Avoid uncontrolled high-cardinality identity partitioning that creates too many tiny partitions/files.

Do not choose a partition scheme only because a source table has a column with a familiar name. Measure query patterns, data skew, file counts, and write behavior.

### 8.4 Partition evolution

Iceberg allows a table's partition spec to change over time. A new partition spec gets a new spec ID. New files use the new spec; existing files keep the spec under which they were written. Readers understand files from multiple specs through metadata.

Example:

```text
Old spec: days(event_ts)
New spec: hours(event_ts)

Historical files -> old day spec
Newly written files -> new hour spec
Reader -> interprets each file using its recorded spec
```

This is a major design benefit: changing partitioning does not automatically require rewriting all historical data.

### 8.5 Partition evolution is not retroactive repartitioning

Changing the default partition spec affects how new files are written. It does not physically reorganize old files. If old data must be physically organized under the new layout for performance, a rewrite/compaction job is a separate maintenance decision.

### 8.6 Partitioning mistakes to avoid

- Too many partitions or too many tiny files.
- Using a transform that does not match common query predicates.
- Excessive bucket counts without measuring file distribution.
- Assuming new partition specs rewrite old files.
- Assuming users must filter on generated partition columns.
- Forgetting that old files and new files can coexist under different spec IDs.

---

# 10. Sorting, metrics, and query planning

## 10.1 Sorting is separate from partitioning

Partitioning groups rows into partition values. Sorting orders rows within written files according to a declared sort order. A sort order has an ID and sort fields, including source field(s), transform, direction, and null ordering.

Sorting can improve data locality and make file-level statistics more selective for common predicates. It does not create a new logical partition and does not replace partition pruning.

A declared default sort order is guidance for writers. The Iceberg specification allows writers not to use it when applying the sort would be prohibitively expensive for that write pattern.

## 10.2 File-level metrics

Iceberg manifests can store metrics including:

- Record count.
- File size.
- Per-column value count.
- Per-column null count.
- NaN count where relevant.
- Lower and upper bounds for values.
- Column sizes and other planning statistics.

These metrics help readers determine that a file cannot satisfy a predicate. For example, if a file's known `event_date` bounds are entirely outside the query range, it can be skipped.

Metrics are not a substitute for reading the file when the metadata is inconclusive. Metrics configuration also has correctness and privacy implications: bounds for sensitive columns can reveal information, so metric collection should be configured deliberately.

## 10.3 Planning versus scanning

A useful performance decomposition is:

1. Catalog and metadata refresh.
2. Snapshot and manifest-list access.
3. Manifest filtering and reading.
4. File-task generation.
5. Data-file open and read cost.
6. Delete-file processing, where applicable.
7. Actual query computation.

When a query is slow, first determine which phase dominates. A large number of small files can make file planning and file-open overhead expensive even when the total data volume is modest.

## 10.4 Manifest rewrite

Manifest rewriting reorganizes manifest metadata to improve planning and file pruning. It does not compact the actual rows in the data files. Do not confuse:

- `rewrite_data_files`: reorganizes data files.
- `rewrite_manifests`: reorganizes metadata manifests.

---

# 11. Updates, deletes, and delete-file behavior

Row-level changes are a key Iceberg interview topic because Iceberg data files are immutable in the normal table-write model.

## 11.1 Two broad write strategies

### Copy-on-write (COW)

The writer reads affected data files, applies the row changes, writes replacement data files, and commits metadata that removes the old files and adds the new ones.

**Advantages:** Readers have less delete-file work because changes are materialized into replacement data files.  
**Costs:** Updating a small number of rows can require reading and rewriting large data files.

### Merge-on-read (MOR)

The writer can add delete information separately from rewriting every affected data file. Readers combine data files with applicable delete information when reading.

**Advantages:** Can reduce write amplification for frequent row-level changes.  
**Costs:** Reads may need to process delete files, and delete-file accumulation can increase read overhead until maintenance rewrites data or delete files.

The exact COW/MOR options and which DML operations support them depend on the engine integration and Iceberg version. Do not claim every engine implements every mode identically.

## 11.2 Position delete files

A position delete identifies a row by:

- The data file path.
- The row's zero-based position within that data file.

Conceptually:

```text
Data file: part-0001.parquet
Deleted rows: positions 3, 17, 28
```

A reader excludes those positions when scanning the referenced data file. Position deletes are tied to particular file locations and positions, so they must be handled carefully when data files are rewritten.

## 11.3 Equality delete files

An equality delete identifies rows by the values of one or more columns, such as `customer_id = 42`. The delete file records the equality field IDs and the values used for matching.

Equality deletes can apply to matching rows in eligible data files in the relevant partition and sequence-number scope. They are not simply a global SQL predicate that a reader applies to every file in the table.

## 11.4 Sequence numbers and delete applicability

In v2/v3, sequence numbers help determine the relative order of data and delete files. A delete should apply to eligible data that existed at the relevant point in the table's change history, not indiscriminately to later inserts that happen to have the same key.

Interview example:

1. A row with `id = 7` is written.
2. A delete for `id = 7` is committed.
3. A new row with `id = 7` is inserted later.

The delete's sequence scope matters. The later insertion must not be treated as though it were the earlier row being deleted merely because the key is the same.

## 11.5 Deletion vectors (v3)

Format v3 introduces binary deletion vectors. A deletion vector represents deleted row positions for a particular data file using a bitmap-like structure. It is designed to make row-position deletes more efficient to process than separate position-delete records in supported implementations.

Important format details:

- Deletion vectors are a v3 capability.
- They are stored using the `deletion-vector-v1` blob definition in the Puffin file format.
- At most one deletion vector is allowed for a given data file in a snapshot.
- A writer adding a deletion vector must merge the relevant earlier position-delete information so a reader can use the vector consistently.
- The current specification says position delete files must not be newly added to v3 tables, although existing position deletes remain valid.

Do not assume that every engine has the same v3 deletion-vector support just because the format specification defines it.

## 11.6 How to explain a MERGE

A SQL `MERGE` is an engine-level command whose implementation changes Iceberg table state. Depending on the chosen write mode and implementation, the result can involve replacement data files, delete files, or a combination. The commit still publishes a new Iceberg snapshot atomically.

A strong answer should describe the physical effect rather than treating `MERGE` as a magical in-place row mutation.

---

# 12. Iceberg format versions

Do not confuse the Apache Iceberg library release with the table-format version stored in a table.

## 12.1 Format-version summary

| Table format version | Main capability | Interview note |
|---|---|---|
| v1 | Core analytic table format with schema/partition metadata and snapshots | Row-level delete files are not part of v1 |
| v2 | Adds row-level delete files, including position and equality deletes | Widely important for row-level update/delete behavior |
| v3 | Adds extended types and capabilities, including nanosecond timestamps, variant/geometry/geography types, default values, multi-argument transforms, row lineage, deletion vectors, and table encryption keys | Check engine support for each feature |
| v4 | Proposed metadata-structure changes, including a root-manifest model | The current spec page says v4 is under active development and not formally adopted; treat as forward-looking |

## 12.2 v1

V1 is the foundational analytic table format. It defines how the table tracks immutable data files and snapshots. It does not support the row-level delete-file mechanism introduced by v2.

## 12.3 v2

V2 adds row-level deletes through delete files. It is the format version commonly associated with position and equality delete semantics.

## 12.4 v3

V3 extends the type system and metadata capabilities. Interview-relevant additions include:

- Nanosecond timestamp types.
- `unknown`, `variant`, `geometry`, and `geography` types.
- Column default-value support.
- Multi-argument partition and sort transforms.
- Row lineage fields, including `_row_id` and `_last_updated_sequence_number`.
- Binary deletion vectors.
- Table encryption-key metadata.

Row lineage deserves careful explanation. It provides stable row identity and last-updated tracking in the v3 table model. It is not, by itself, a promise that every update path preserves a row's ID. The spec notes that updates implemented through equality deletes are treated as removal plus addition because the writer may not read the original row to preserve its prior row ID.

## 12.5 v4: how to discuss it responsibly

The current specification describes a proposed v4 metadata layout in which snapshots point to a root manifest rather than the v1–v3 manifest-list structure. It also describes Parquet root and leaf manifests and other format changes. However, the same spec page says v4 is still under active development and has not been formally adopted.

In an interview, say:

> “I understand the direction of v4, but I would not design a production dependency on it until the format spec is adopted and the exact Iceberg library, catalog, and engine versions I use support it.”

This is safer and more accurate than presenting a developing specification as a universally available table format.

---

# 13. Catalogs and table registration

## 13.1 What a catalog does

An Iceberg catalog provides the namespace for table identifiers and resolves a table name to the current table metadata location. It also participates in the atomic metadata commit process.

The catalog is not the same thing as the table's data files or the complete metadata tree. The metadata files remain part of the Iceberg table representation in storage.

## 13.2 Common catalog implementations

The Iceberg project documents multiple catalog implementations, including:

- Hadoop catalog.
- Hive catalog.
- JDBC catalog.
- REST catalog.
- Cloud/service-specific catalog integrations such as AWS Glue.
- Custom catalog implementations.

The operational characteristics differ. When selecting a catalog, assess atomic commit behavior, availability, security integration, namespace management, interoperability, and how clients discover the current metadata location.

## 13.3 REST catalog

The Iceberg REST catalog protocol standardizes catalog interactions over an HTTP API. Its purpose is to provide a common integration contract for clients and catalog services. A REST catalog does not change the core Iceberg table metadata model: the table is still represented by Iceberg metadata and data files.

## 13.4 Catalog versus storage

A common senior-level question is: “If the data is in object storage, why do I need a catalog?”

Answer: the catalog provides a stable table identifier and a coordinated current-metadata pointer. Object storage holds the table's data and metadata files. A reader needs a reliable way to find the current metadata file; a writer needs an atomic way to publish the next metadata state.

## 13.5 Direct path versus catalog identifier

Some Iceberg integrations can load a table from a direct path, while catalog-based access uses a logical identifier such as `catalog.database.table`. Catalog-based access is generally easier to govern and share across clients. The exact registration and path-loading behavior is integration-specific.

---

# 14. Branches and tags

Iceberg snapshots form a history. Branches and tags provide named references into that history, each with its own retention behavior.

## 14.1 Branch

A branch points to the head of a snapshot lineage. New commits to that branch can advance its head. Branches are useful for:

- Testing a new ingestion or transformation workflow.
- Validating data before publishing it to the main branch.
- Isolating experimental data changes.
- Keeping a separately retained history for a workflow.

## 14.2 Tag

A tag names a particular snapshot. It is useful for:

- Audit checkpoints.
- Important monthly or quarterly snapshots.
- Pinning a known-good historical table state.
- Retaining a snapshot for a defined period.

## 14.3 Branch versus tag

| Branch | Tag |
|---|---|
| Points to the head of a snapshot lineage | Points to a particular snapshot |
| Can advance as new commits are made to the branch | Used as a named historical checkpoint |
| Supports isolated write/test workflows | Commonly used for audit or release-style retention |
| Has branch-level snapshot retention rules | Has tag/reference retention rules |

## 14.4 Write-audit-publish pattern

A common pattern is:

1. Enable the relevant write-audit-publish setting.
2. Create an audit branch from an intended starting snapshot.
3. Write candidate data to that branch.
4. Run validation against the branch state.
5. If validation succeeds, fast-forward the main branch to the audit branch's head.
6. Retain or expire the audit reference according to policy.

This pattern reduces the risk of publishing unvalidated writes to the main table history. Support and command syntax depend on the engine integration.

## 14.5 Retention interaction

Branches and tags affect snapshot expiration. A snapshot referenced by a retained branch or tag may remain reachable even when it is old. Retention design must account for all references, not only the age of snapshots on the main branch.

---

# 15. Maintenance and table health

Iceberg maintenance is essential for long-running tables. Successful commits can still produce a table that becomes operationally inefficient if files, manifests, delete files, or historical metadata accumulate without a plan.

## 15.1 Compact data files

**Operation:** rewrite small data files into fewer, appropriately sized files; optionally split oversized files or organize data according to a desired sort order.

**Why it matters:**

- Fewer file opens.
- Less manifest metadata.
- Fewer scan tasks.
- Better data locality when sorting is used.
- Reduced planning and scheduling overhead.

The target file size is a workload-specific setting, not a universal number. Consider the file format, compression, executor/task behavior, storage characteristics, and read patterns.

## 15.2 Rewrite manifests

**Operation:** rewrite and reorganize manifest files.

**Why it matters:**

- Improves manifest pruning and scan planning.
- Can cluster manifest entries in an order useful for common filters.
- Reduces metadata-planning overhead in tables with many incremental commits.

It does not rewrite the actual data rows.

## 15.3 Rewrite delete files

Position delete files can accumulate and become expensive to process. Where supported and applicable, rewrite them into more efficient delete files and remove dangling delete references. Compaction of data files can also materialize changes and reduce dependence on delete files.

The available delete-file rewrite operations depend on the format version and implementation. For v3 deletion vectors, use the maintenance capabilities actually supported by the deployed version rather than assuming a v2 position-delete workflow applies unchanged.

## 15.4 Expire snapshots

Snapshot expiration removes snapshots outside the configured retention policy and can delete data files that are no longer needed by any retained snapshot/reference.

Example Spark procedure:

```sql
CALL prod_catalog.system.expire_snapshots(
  table => 'analytics.events',
  older_than => TIMESTAMP '2026-10-01 00:00:00',
  retain_last => 20
);
```

The exact parameters and defaults must be checked for the installed procedure version. Snapshot expiration is irreversible for the expired history: the corresponding snapshots are no longer available for time travel.

## 15.5 Remove orphan files

Orphan files are storage files not referenced by valid Iceberg metadata. They can be left behind by failed jobs, abandoned tasks, or interrupted writes.

Example dry run:

```sql
CALL prod_catalog.system.remove_orphan_files(
  table => 'analytics.events',
  dry_run => true
);
```

A dry run should be part of the operational procedure before actual deletion.

**Important safety rule:** Do not use an aggressive orphan-file age threshold. A file may be written before a long-running job commits. The cleanup threshold must safely exceed the maximum expected duration of active writes and relevant commit delays. Validate the catalog/table location and path-prefix settings before deleting files.

## 15.6 These operations solve different problems

| Maintenance action | Main target | Typical symptom |
|---|---|---|
| Rewrite data files | Data-file layout | Many small files, file-open overhead, poor locality |
| Rewrite manifests | Metadata-file layout | Slow planning, excessive or poorly organized manifests |
| Rewrite delete files | Row-level delete metadata | Many or large delete files, high read overhead |
| Expire snapshots | Historical table state | Excessive retained history and storage use |
| Remove orphan files | Unreferenced storage objects | Files left by failed or abandoned writes |

Do not use snapshot expiration as a substitute for orphan cleanup, and do not use orphan cleanup as a substitute for snapshot expiration.

## 15.7 A safe maintenance approach

A production maintenance policy should:

1. Define time-travel, audit, rollback, and branch/tag retention requirements.
2. Observe file-size distribution, file count, manifest count, delete-file count, and planning time.
3. Compact data when small files or poor data locality justify it.
4. Rewrite manifests when metadata layout causes planning overhead.
5. Rewrite/delete stale delete files when supported and needed.
6. Expire snapshots only after confirming retention requirements.
7. Run orphan cleanup conservatively, preferably with a dry run first.
8. Monitor failures, duration, bytes rewritten/deleted, and table health after each operation.

---

# 16. Streaming and incremental processing

Iceberg supports both batch-style table operations and streaming-oriented integration patterns, but the exact semantics are connector-specific.

## 16.1 Streaming writes

A streaming writer periodically commits new table state. Each successful data-changing commit creates a new snapshot. Very frequent commits can create many snapshots, manifests, and small files if the writer emits tiny batches.

Tune commit frequency and file sizing together. Lower latency can increase metadata and small-file pressure; larger batches can improve file sizes but increase data-visibility latency.

## 16.2 Incremental reads

An incremental scan can request appended data between a starting and ending snapshot ID. This is useful for processing newly appended records without scanning the complete table every time.

However, the supported incremental semantics depend on the engine and API. For example, the documented Spark incremental read is append-oriented and does not represent every possible overwrite or delete change as a complete change-data stream. Do not describe an append-only incremental scan as universal CDC for all table operations.

## 16.3 Structured Streaming caveat

The current Spark integration documentation states that streaming reads process append snapshots by default; overwrite and delete snapshots require explicit handling and otherwise cause exceptions. This is a critical interview caveat: a streaming read of an Iceberg table is not automatically equivalent to consuming every row-level change in the table.

## 16.4 Streaming table maintenance

For continuously written tables, maintenance must be planned as part of the pipeline:

- Control commit frequency.
- Avoid emitting excessive tiny files.
- Compact data files on a schedule appropriate to write volume and query latency.
- Rewrite manifests when planning overhead grows.
- Expire snapshots with a retention policy compatible with downstream consumers.
- Do not expire snapshots still needed by incremental readers or audit workflows.

---

# 17. Performance engineering

Senior interviews often test whether you can diagnose the cause of poor performance rather than listing generic tuning settings.

## 17.1 File-size distribution

Inspect the number of files and their size distribution. Many tiny files increase file-open cost, scan-task overhead, manifest size, and planning time. A few oversized files can reduce parallelism and increase the amount of data read for selective workloads.

Choose a target based on the workload, not a memorized number. Measure actual output file sizes after compression.

## 17.2 Partition design

Good partitioning narrows the candidate file set without creating excessive partition cardinality. Start from real filter predicates, estimate data distribution, and validate pruning with metadata and query plans.

A common mistake is partitioning by every frequently queried column. High-cardinality identity partitioning can produce many tiny partitions and files. Bucket transforms can help distribute high-cardinality keys but still need an appropriate bucket count.

## 17.3 Sorting and clustering

For queries that repeatedly filter on a non-partition column, a suitable sort order can improve file-level min/max selectivity. For multi-column predicates, consider the order of sort fields and the actual data distribution. Verify that writers apply the intended order and that file metrics show improved pruning.

## 17.4 Metadata overhead

If a query spends too much time planning, inspect:

- Snapshot count and metadata history.
- Manifest count and size.
- Number of files in the table.
- Manifest-level partition summaries.
- File-level metrics availability.
- Frequency of streaming/short commits.

Consider manifest rewriting and file compaction only after identifying which metadata or file-count issue is responsible.

## 17.5 Delete-file overhead

With merge-on-read patterns, evaluate the number and size of applicable delete files, the ratio of deleted rows to data rows, and the frequency of rewrites. A table with many row-level deletes may require data-file compaction or delete-file rewriting to keep read cost controlled.

## 17.6 Read/write trade-off

- COW usually spends more work during writes to reduce delete processing at read time.
- MOR can reduce immediate write amplification, but shifts work to reads and later maintenance.

Choose using the workload's read/write ratio, update frequency, latency objective, and engine support.

## 17.7 Performance troubleshooting sequence

When a query is slow:

1. Confirm it is reading the intended snapshot and table reference.
2. Inspect scan planning time versus data-reading time.
3. Check the number and size distribution of candidate files.
4. Check whether partition pruning is effective.
5. Check whether column metrics are available and useful.
6. Inspect manifest count and layout.
7. Inspect applicable delete files.
8. Check sort order and file locality.
9. Change one factor, then measure again.

---

# 18. Concurrency, correctness, and failure handling

## 18.1 Concurrent append jobs

Two append jobs can often both succeed through optimistic retries because their newly created files can be added to the latest table state without invalidating each other's assumptions. The writer that loses the first metadata race refreshes and retries.

## 18.2 Concurrent overwrite and append

Whether operations can be safely combined depends on the overwrite's scope and validation rules. A predicate overwrite may conflict with a concurrent write that adds data matching the overwritten predicate. The writer must not assume that an append is always compatible with every overwrite.

## 18.3 Concurrent compaction and data modification

Compaction replaces source files with rewritten files. If a concurrent writer changes or removes one of those source files, compaction must validate its assumptions before committing. This prevents a rewrite from accidentally losing a concurrent update.

## 18.4 Commit conflict versus task failure

These are different failure classes:

- **Task/write failure:** Some data tasks fail before commit. The job may leave unreferenced files.
- **Commit conflict:** Files may have been written successfully, but another writer changed current metadata first. The writer must refresh and validate/retry.
- **Validation failure:** The refreshed table no longer satisfies the assumptions required by the operation. The operation must fail or be replanned.
- **Catalog/metadata service failure:** The commit outcome may need careful reconciliation. Never assume a timeout means the commit definitely failed; refresh table metadata and inspect the committed state before blindly replaying a non-idempotent operation.

## 18.5 Failed commit and orphan files

If files were written but the metadata commit never made them part of a valid snapshot, they may become orphans. Do not immediately delete them based on a task failure message alone. First determine whether the commit may have succeeded, then let a conservative orphan-cleanup process identify unreferenced files.

## 18.6 Missing data file

If metadata references a file that no longer exists, readers may fail when they try to open it. This is not a normal snapshot-isolation event. Investigate manual deletion, lifecycle policies, cleanup jobs, permissions, path changes, and storage reliability. Iceberg metadata cannot reconstruct a physically deleted data file by itself.

## 18.7 Unsafe manual deletion

Never remove data, delete, manifest, or metadata files manually merely because their names look old. Determine reachability through Iceberg metadata and use supported maintenance operations. Manual deletion can break current queries and historical snapshots.

## 18.8 Object-store behavior

Iceberg's design avoids using directory listing as the source of truth for table membership and avoids requiring the traditional rename-based commit pattern in its metadata-table commit path. This makes the design suitable for object stores. The catalog's atomic metadata update remains essential.

---

# 19. Practical Iceberg SQL examples

The following are **Spark SQL integration examples**, not universal SQL syntax for every engine. Catalog names, procedure availability, and exact DDL support vary by deployed versions.

## 19.1 Create a table

```sql
CREATE TABLE prod.analytics.orders (
  order_id     BIGINT,
  customer_id  BIGINT,
  order_ts     TIMESTAMP,
  status       STRING,
  total_amount DECIMAL(18, 2)
)
USING iceberg
PARTITIONED BY (days(order_ts), bucket(32, customer_id))
TBLPROPERTIES (
  'format-version' = '2'
);
```

## 19.2 Insert data

```sql
INSERT INTO prod.analytics.orders
VALUES
  (1001, 25, TIMESTAMP '2026-11-01 10:00:00', 'CREATED', 250.00),
  (1002, 27, TIMESTAMP '2026-11-01 10:05:00', 'PAID',    400.00);
```

A successful data-changing commit publishes a new snapshot.

## 19.3 Add a column

```sql
ALTER TABLE prod.analytics.orders
ADD COLUMN source_system STRING;
```

This is a schema evolution operation. It does not require rewriting all old data files. Historical rows resolve the new field according to Iceberg's schema/default rules.

## 19.4 Rename a column

```sql
ALTER TABLE prod.analytics.orders
RENAME COLUMN status TO order_status;
```

The stable field ID preserves the logical identity of the renamed field.

## 19.5 Query snapshots and history

```sql
SELECT *
FROM prod.analytics.orders.snapshots;

SELECT *
FROM prod.analytics.orders.history;
```

## 19.6 Inspect files and manifests

```sql
SELECT *
FROM prod.analytics.orders.files;

SELECT *
FROM prod.analytics.orders.manifests;

SELECT *
FROM prod.analytics.orders.partitions;

SELECT *
FROM prod.analytics.orders.refs;
```

Metadata-table availability and field names vary by integration and version. Treat these as diagnostic surfaces, not user-facing business tables.

## 19.7 Time travel

```sql
SELECT *
FROM prod.analytics.orders VERSION AS OF 1234567890123;

SELECT *
FROM prod.analytics.orders TIMESTAMP AS OF '2026-11-01 23:59:59';
```

## 19.8 Roll back

```sql
CALL prod_catalog.system.rollback_to_snapshot(
  'analytics.orders',
  1234567890123
);
```

## 19.9 Rewrite data files

```sql
CALL prod_catalog.system.rewrite_data_files(
  table => 'analytics.orders'
);
```

This rewrites selected data files according to the procedure's strategy and options. It is not the same as rewriting manifests.

## 19.10 Rewrite manifests

```sql
CALL prod_catalog.system.rewrite_manifests(
  'analytics.orders'
);
```

This reorganizes manifest metadata for planning efficiency.

## 19.11 Expire snapshots

```sql
CALL prod_catalog.system.expire_snapshots(
  table => 'analytics.orders',
  older_than => TIMESTAMP '2026-10-01 00:00:00',
  retain_last => 20
);
```

Before running this, confirm required time-travel, rollback, branch, tag, and downstream-reader retention.

## 19.12 Remove orphan files safely

```sql
CALL prod_catalog.system.remove_orphan_files(
  table => 'analytics.orders',
  dry_run => true
);
```

Review the candidates and the configured age threshold before running a deletion operation.

---

# 20. Senior-level design scenarios

Use these scenarios to practise structured answers. A good response states the symptom, identifies likely causes, describes how to inspect Iceberg metadata, recommends a safe action, and explains the trade-off.

## Scenario 1: A table has millions of tiny files

**Likely causes:** Small streaming batches, frequent commits, insufficient write distribution, or too many small output tasks.

**Investigation:** Inspect `files`, file-size distribution, snapshot/commit frequency, and query planning time.

**Plan:** Adjust write batch/file sizing where possible, then compact data files. Monitor rewritten bytes, output sizes, and query improvements. Do not expire snapshots just to solve small-file overhead; that solves a different problem.

## Scenario 2: Queries spend longer planning than scanning

**Likely causes:** Too many manifests, too many files, poor manifest clustering, or insufficient pruning metadata.

**Investigation:** Inspect manifests, file counts, partition summaries, metrics, and planning time.

**Plan:** Rewrite manifests if metadata organization is the issue; compact data files if file count is the issue; improve metrics collection where appropriate. Re-measure after each change.

## Scenario 3: A daily partition is no longer selective enough

**Likely cause:** Query patterns now need more granular time filtering.

**Plan:** Evolve the partition spec to an hourly transform for new files. Existing files remain under their old spec. If historical files also need the new physical layout, schedule a separate rewrite based on cost and benefit.

## Scenario 4: Two ingestion jobs commit simultaneously

**Plan:** Explain optimistic concurrency. Each writer prepares metadata and attempts the atomic commit. A loser refreshes and validates whether its work can be retried. Compatible appends may both commit; conflicting overwrites or file replacements may fail validation. Ensure the job handles retryable and non-retryable failures distinctly.

## Scenario 5: A delete-heavy table becomes slow to read

**Likely cause:** Accumulated delete-file processing and/or inefficient file layout.

**Investigation:** Inspect delete-file counts and sizes, affected data files, write mode, and scan overhead.

**Plan:** Depending on format version and engine support, rewrite delete files and/or compact affected data files to materialize changes. Balance write amplification against future read cost.

## Scenario 6: A bad batch was committed

**Plan:** Identify the bad snapshot and the last known-good retained snapshot. Assess downstream consumers and branch/tag references. Use time travel to validate the intended recovery point, then perform a controlled rollback or publish a corrective snapshot. Preserve the bad snapshot if audit requirements demand it; do not expire it before the incident investigation is complete.

## Scenario 7: Storage usage keeps increasing

**Likely causes:** Retained snapshots, branches/tags, old metadata files, orphan files, and normal data growth.

**Investigation:** Separate reachable historical files from unreferenced orphan files. Inspect snapshot age/count and branch/tag retention.

**Plan:** Right-size retention, expire eligible snapshots, remove old metadata files when safe, and clean orphan files conservatively. Do not manually delete files by path age alone.

## Scenario 8: A streaming consumer misses or rejects table changes

**Likely cause:** The consumer expects all row-level changes, but its incremental/streaming read path only supports append snapshots or does not support overwrite/delete snapshots.

**Plan:** Verify the exact connector semantics. Decide whether the requirement is append-only incremental ingestion or full change processing. Do not claim append-only incremental scans capture all deletes and overwrites.

## Scenario 9: A rename causes confusing values in a downstream reader

**Likely causes:** Reader/writer incompatibility, missing Iceberg field-ID handling, name mapping problems for imported files, or an engine that is not reading the table through the correct Iceberg integration.

**Plan:** Inspect the Iceberg schema and field IDs, verify the relevant reader compatibility, validate name mappings where files lack Iceberg field IDs, and test against historical files before broad rollout.

## Scenario 10: An orphan cleanup job deletes files still being written

**Likely cause:** The cleanup age threshold is shorter than the longest active write or commit delay, or the cleanup scope/path is incorrect.

**Plan:** Stop the cleanup, assess affected tables/snapshots, and restore missing objects if possible. Set a conservative threshold exceeding maximum writer duration and validate candidates through dry runs. Coordinate cleanup with writer schedules and table ownership.

## Scenario 11: You need a test-before-publish workflow

**Plan:** Use an Iceberg branch to isolate candidate writes, validate data against that branch, and fast-forward the main branch when approved. Define retention for the branch and its snapshots. Check schema compatibility because the table schema is shared across branches.

## Scenario 12: You need a historical audit checkpoint

**Plan:** Create a tag on the required snapshot and give it an explicit retention policy. Verify snapshot expiration respects the reference. Document that a tag preserves a snapshot reference; it is not a separate copy of the table's data.

---

# 21. Interview questions with model answers

## Beginner level

### 1. What is Apache Iceberg?

Apache Iceberg is an open table format for large analytic tables stored as files. It tracks table state through metadata and snapshots, enabling atomic commits, consistent reads, schema evolution, partition evolution, and time travel.

### 2. Is Iceberg a query engine?

No. Iceberg is a table format and metadata-management layer. A compute engine performs the actual query execution and must implement the relevant Iceberg integration.

### 3. What is a snapshot?

A snapshot is a consistent state of the table at a point in time. It resolves to the complete set of live data files for that state and is the basis for time travel and reader isolation.

### 4. What is a manifest?

A manifest is an immutable metadata file that tracks a subset of data files or delete files, including paths, partition values, and metrics.

### 5. What is a manifest list?

For v2/v3, it is a metadata file associated with a snapshot that lists the manifests and includes statistics useful for pruning manifests during planning.

### 6. What does a catalog do?

It resolves a logical table identifier to the current metadata location and participates in coordinating atomic metadata commits.

### 7. What is hidden partitioning?

Iceberg derives partition values from source columns using transforms. Users filter on the source columns and do not have to manage derived partition columns in their queries.

### 8. Can a table's schema change without rewriting all data files?

Yes. Schema evolution is primarily a metadata change. Stable field IDs let Iceberg interpret old files under the new logical schema.

## Intermediate level

### 9. Explain the Iceberg metadata hierarchy.

A catalog resolves the table's current metadata JSON. That metadata identifies the current snapshot. In v2/v3 the snapshot points to a manifest list, which lists manifests. The manifests track the data and delete files that make up the snapshot.

### 10. How does Iceberg provide atomic writes?

A writer writes new files and metadata first, then atomically replaces the current table metadata location. Files become visible as part of the table only after the metadata commit succeeds.

### 11. What is optimistic concurrency control?

Writers prepare changes based on a read snapshot and attempt to commit without holding a long-lived global lock. If another writer commits first, the writer refreshes, validates its assumptions, and retries if safe. Otherwise it fails with a conflict.

### 12. How does time travel work?

A query selects a retained snapshot by ID or timestamp. Iceberg reads the files referenced by that snapshot. Time travel ends when the snapshot or required files are expired or removed.

### 13. What is the difference between snapshot expiration and orphan-file cleanup?

Snapshot expiration removes old history and files no longer required by retained snapshots. Orphan-file cleanup removes files that are not referenced by valid table metadata, often after failed writes. They solve different problems.

### 14. What is partition evolution?

A table can adopt a new partition spec without rewriting all old data. New files use the new spec; old files retain their original spec ID. Readers interpret both.

### 15. Why are field IDs important?

They identify fields independently of their names and positions. A rename or reorder therefore does not cause historical values to be assigned to the wrong logical column.

### 16. Explain position deletes and equality deletes.

Position deletes identify a row by file path and row position. Equality deletes identify rows using one or more column values identified by field IDs. Both are delete-file mechanisms introduced in v2; later format versions add further delete capabilities.

### 17. What is copy-on-write versus merge-on-read?

Copy-on-write rewrites affected data files during the write. Merge-on-read can record delete information separately and apply it during reads. COW spends more during writes; MOR can shift work to reads and maintenance.

### 18. Why can a table with few terabytes still have poor query performance?

The table may have too many small files or manifests, inefficient partitioning, weak file-level pruning metrics, or many applicable delete files. Data volume alone does not determine planning and file-open cost.

## Advanced level

### 19. What is the difference between a snapshot and a sequence number?

A snapshot identifies a table state. A sequence number orders changes in the table history and participates in determining row-level delete applicability. They are related but not interchangeable.

### 20. Why does a delete not necessarily affect a later insert with the same key?

Delete applicability is scoped using file/partition relationships and sequence-number semantics. A later insertion is a later table change and must not be incorrectly treated as the previously deleted row solely because its equality key matches.

### 21. What happens if a writer loses the commit race?

It refreshes current metadata and validates the assumptions behind its operation. If the new table state is compatible, it can retry using the latest metadata. If the operation's target files or predicate assumptions are invalidated, it must fail or be replanned.

### 22. Why can compaction conflict with a concurrent write?

Compaction replaces a known set of source files. If another operation modifies or removes one of those files before compaction commits, the compaction's assumptions may no longer hold. Iceberg validates this to prevent lost updates.

### 23. What is row lineage in v3?

V3 adds `_row_id` and `_last_updated_sequence_number` tracking to provide row identity and last-update sequence information. It does not mean all update paths preserve row identity; equality-delete-based updates can be represented as removal plus insertion.

### 24. What is a deletion vector?

A deletion vector is a bitmap-style representation of deleted row positions associated with a data file. It is a v3 capability and is intended to make position-delete processing more efficient in supported readers/writers.

### 25. Is v4 a production-ready format version?

The current Iceberg specification page says v4 is under active development and has not been formally adopted. A senior engineer should distinguish emerging implementation work from an adopted format contract and verify end-to-end compatibility before using it.

### 26. Does a schema change create a new snapshot?

Not necessarily. Metadata-only schema changes update table metadata, while a data-changing operation normally creates a new snapshot. A snapshot and a metadata JSON version are different concepts.

### 27. Does partition evolution repartition historical data?

No. It changes the default partition spec for new files. Old files stay associated with their original spec. Rewriting historical files is a separate operation.

### 28. Why is object-store listing not the source of truth for Iceberg?

Iceberg tracks files explicitly through metadata. Readers do not reconstruct table state by listing every file under a storage prefix. This reduces reliance on listing consistency and avoids making directory layout the definition of table contents.

### 29. What is the risk of expiring snapshots too aggressively?

You can lose required time-travel and rollback points, remove data needed by audit consumers, or expire history needed by incremental processing. Branches and tags can also affect which snapshots remain retained.

### 30. What is the risk of running orphan cleanup too aggressively?

It can delete files belonging to an active but not-yet-committed write or misclassify files because of an incorrect scope/path. Use a conservative age threshold, a dry run, and operational coordination with writers.

### 31. How would you investigate a slow Iceberg query?

Separate planning from scanning. Inspect the selected snapshot, number of candidate files, file sizes, partition pruning, manifest count/layout, metrics availability, applicable delete files, and sort order. Then target the maintenance or design issue supported by the evidence.

### 32. How would you design retention for an important table?

Start from recovery, audit, legal, time-travel, and downstream-reader requirements. Set snapshot and branch/tag retention accordingly, measure storage impact, and only then configure automated snapshot expiration and orphan cleanup. Keep cleanup age greater than the maximum active write duration.

---

# 22. Common misconceptions and traps

| Misconception | Correct understanding |
|---|---|
| Iceberg is a storage engine | It is a table format and metadata-management model; compute and storage are separate components |
| A table is all files under its directory | The authoritative table contents are defined by metadata and snapshots |
| A snapshot is just the files written by the last job | A snapshot represents the complete visible table state |
| A manifest is a data file | A manifest is metadata about data or delete files |
| A manifest belongs to one physical partition | A manifest may track files across partitions; entries carry partition tuples |
| Renaming a column requires rewriting all old files | Field IDs preserve logical identity across a rename |
| Changing the partition spec repartitions old files | New files use the new spec; historical files retain their old spec unless rewritten |
| A successful task means the table commit succeeded | Files can be written before the metadata commit; publication is the commit boundary |
| Every concurrent append conflicts permanently | Compatible operations can refresh and retry; conflicts depend on validation assumptions |
| Snapshot expiration and orphan cleanup are the same | One removes unneeded historical state; the other removes unreferenced files |
| Time travel works forever | It works only while the selected snapshot and required files remain retained |
| Every incremental read is full CDC | Some integrations only expose append snapshots or have limited change semantics |
| V3 deletion vectors are supported by every engine | Format capability and engine implementation support must both be verified |
| The software version `1.12.0` means table format v12 | Library release numbering and table-format versioning are separate |
| V4 is already a universally adopted format | The current specification labels v4 as under development and not formally adopted |

---

# 23. A 30-second and a 2-minute interview answer

## 30-second answer

“Apache Iceberg is an open table format for large analytic tables stored as files. It tracks table contents through metadata and immutable snapshots rather than relying on directory listing. A catalog resolves the table to its current metadata file; the metadata identifies the current snapshot; and manifests track the data and delete files that make up that snapshot. Writers write files first and then atomically publish a new metadata state using optimistic concurrency. This gives consistent snapshot reads, time travel, rollback, schema evolution through field IDs, and partition evolution through hidden transforms.”

## 2-minute answer

“An Iceberg table is a metadata-defined table. The catalog gives clients a logical table identifier and the current metadata location. The table metadata records schema history, partition specs, properties, snapshots, and references. For established v2/v3 tables, a snapshot points to a manifest list, which points to manifests. The manifests track file paths, partition tuples, sequence information, and file metrics. The snapshot represents the complete live data-file set at that point in history, and manifests can be reused across snapshots.

For reads, the engine selects a snapshot and uses manifest summaries, partition values, and column metrics to prune manifests and files before reading rows. For writes, tasks create new data and possibly delete files, then the writer validates its assumptions and atomically updates the catalog's current metadata pointer. Concurrent writers use optimistic concurrency: a writer that loses the metadata race refreshes and retries only if its operation remains valid.

The main capabilities I would highlight are snapshot isolation, time travel and rollback, stable field IDs for schema evolution, hidden partitioning and partition evolution, row-level delete files in v2, and deletion vectors/row lineage in v3. In production I would also plan for small-file compaction, manifest rewriting, snapshot expiration, orphan cleanup, and careful branch/tag retention. I would verify library and engine support for each feature rather than relying on the table-format specification alone.”

---

# 24. Final revision checklist

You are ready for most Iceberg interviews when you can explain these without notes:

- [ ] Iceberg is a table format, not a query engine.
- [ ] The catalog resolves the current metadata location and coordinates commits.
- [ ] The metadata JSON, snapshot, manifest list, manifest, data file, and delete file are different things.
- [ ] A snapshot represents the complete table state, not just the latest batch.
- [ ] The commit boundary is the atomic metadata publication, not the data-file write.
- [ ] Optimistic concurrency means refresh, validate, and retry only when safe.
- [ ] Field IDs make schema evolution safe across old and new files.
- [ ] Hidden partitioning lets queries use source columns without exposing partition columns.
- [ ] Partition evolution changes the spec for new files; it does not rewrite old files.
- [ ] File metrics and manifest summaries enable pruning.
- [ ] COW and MOR trade write amplification against read-time delete processing.
- [ ] V2 adds position and equality delete files; v3 adds deletion vectors and row lineage.
- [ ] Snapshot expiration and orphan cleanup solve different problems.
- [ ] Branches and tags affect snapshot reachability and retention.
- [ ] Streaming/incremental read semantics are integration-specific and may be append-only.
- [ ] Library release version and table format version are not the same.
- [ ] V4 must be described as developing until formally adopted and supported by the deployed stack.
