# Architecture
## 1. Data Layer
The Apache Iceberg data layer constitutes the physical foundation of a table, storing actual data in formats like Parquet, ORC, or Avro alongside delete files for row-level mutations. This layer relies on scalable infrastructure such as cloud object storage or HDFS
### 1.1 Data Files
The physical data layer of an Apache Iceberg table relies on file-format agnosticism, natively supporting Parquet, ORC, and Avro. This neutrality accommodates legacy organizational data, fits varying workload demands (e.g., Avro for streaming vs. Parquet for OLAP), and future-proofs infrastructure against changing industry standards.  

**Why Parquet Dominates the Data Layer**  

While Iceberg is flexible, Apache Parquet is the de facto industry standard for production data lakes due to its highly efficient columnar structure:
* **High Parallelism**: A single Parquet file can be split multiple ways, allowing different compute threads to read sections of the file simultaneously.
* **Vectorized Data Skipping**: It stores granular column-level statistics (min/max values, null counts) at multiple split points, letting engines skip irrelevant data entirely.
* **Superior Compression**: Grouping similar column data types together enables dense algorithmic compression, drastically reducing storage costs and boosting read throughput.
### 1.2 Delete Files
Because data lake storage is immutable, rows cannot be edited in place. To handle updates and deletions, Apache Iceberg (v2 format) introduces Delete Files to enable the Merge-on-Read (MoR) strategy. Instead of rewriting an entire data file, MoR writes a separate, compact delete file that query engines merge with the base data file at runtime.   

There are two types of Delete Files
#### 1.2.1 Positional Delete Files
Positional delete files log logical deletions by storing the absolute file path and the specific row index number for every deleted record. When a query engine reads the table, it references this exact physical address to omit the deleted rows from the final result set in real time.  

**Key Characteristics**

* **Physical Mapping:** Maps deletions directly to data files via string paths and integer row offsets.
* **Read-Optimized MoR:** Minimizes runtime compute overhead because the engine knows exactly which rows to skip without evaluating column conditions.
* **Write Overhead:** Requires the writing engine to search, locate, and track the precise physical row positions before creating the delete file.
#### 1.2.2 Equality Delete Files
Equality delete files identify logically deleted rows by storing specific column values (e.g., order_id = 1234) rather than physical file locations. When a query engine reads the dataset, it scans these values and filters out any matching rows from the final result set in real time.  

**Key Characteristics**

* **Write-Optimized MoR:** Ingestion engines can write deletes instantly without needing to scan existing data files to find physical row positions.
* **Primary Key Alignment:** This method is highly effective for transactional tables with unique identifiers, allowing a single ID to mask a specific row.
* **Mass Deletions:** It natively supports broad categorical deletes by targeting a single field value that applies to thousands of rows simultaneously.
* **Read-Time Overhead:** This approach shifts the performance tax to the reader, as the query engine must perform a join-like operation to evaluate data rows against equality conditions at runtime.

To prevent an equality delete file from accidentally erasing future insertions with identical column values, Apache Iceberg uses monotonic sequence numbers. Every table modification (commit) receives a unique, incrementing sequence number. Query engines use these numbers to ensure a delete file is only applied to older data files, completely ignoring newly inserted rows that share the same identifier.

**How Sequence Numbers Work (The Ordering Lifecycle)**  

Iceberg applies sequence numbers across commits to maintain a perfect chronological ledger:

* Initial State (Seq 1): Base data files are written and tagged with sequence number 1.
* The Deletion (Seq 2): An equality delete file is committed with sequence number 2 to remove a specific ID.
* The Re-Insertion (Seq 3): The same ID is re-inserted. The new data file is tagged with sequence number 3.
* The Query Resolution: The reading engine applies the delete file (Seq 2) exclusively to data files with a sequence number less than 2. Because the new insertion is at sequence 3, it safely bypasses the delete filter and shows up correctly in your query results.
  
**Key Benefits**  

* **Correctness**: Guarantees absolute transactional consistency for streaming upserts and rapid write cycles.
* **Zero Rewrites**: Eliminates the need to modify or rewrite existing delete files when new data arrives.
* **Deterministic Reads**: Allows multiple engines to independently construct the exact logical state of a table at any snapshot.
<br></br>
## 2. Metadata Layer
The Iceberg metadata layer is a colocated tree structure stored alongside your physical data files. It functions as the management system of the table by tracking all data files, file-level statistics, and the historical operations that created them. Composed of three distinct file types **manifest files**, **manifest lists**, and **metadata files** this layer is the engine that enables Iceberg's core features, including time travel, schema evolution, and efficient large-scale query planning.
### 2.1 Manifest Files
A manifest file is an immutable, Avro-formatted metadata file that acts as the leaf-level index of an Iceberg table. Its primary purpose is to track table data at the individual file level (rather than the folder or partition level), which solves the core scaling limitations of the traditional Hive table format.  
Manifest files sit at the lowest layer of the Iceberg metadata tree, directly above the physical data layer.  

**Strict Separation of Concerns**  

While data files and delete files utilize the exact same structural schema, they are kept entirely separate:

* **Data Manifests:** Track only data files (e.g., Parquet, ORC).
* **Delete Manifests:** Track only positional or equality delete files.
* Note: A single manifest file will never mix both file types.

**Embedded Metadata & Statistics**  

Instead of tracking entire folders, each row in a manifest file represents a specific physical file and stores:

* **Physical Details:** Absolute URI file path, file format, and file size in bytes.
* **Partition Membership:** The exact partition values to which that file belongs.
* **Row Counts:** The total number of records contained within that specific file.
* **Column-Level Statistics:**
* **Lower and Upper Bounds** (Minimum and maximum values for every column).
   * Null and NaN Counts (The total number of null or floating-point missing values).

| Feature | Old Hive Table Format | Apache Iceberg Manifests |
|---|---|---|
| Tracking Level | Directory/Folder Level | File Level |
| Stats Collection | Expensive, post-processed read jobs over entire partitions. | Lightweight, inline writes bundled by the engine during ingestion. |
| Metadata Freshness | Stale or missing (because jobs are too costly to re-run). | Always accurate and up-to-date with every data commit. |
| File Checking | Engines must open the footers of hundreds of files. | Engines read one manifest to check stats for multiple data files. |

**Key Performance Benefits**

* **Eliminates Storage Overhead:** Query engines skip the slow, expensive process of opening physical data file footers just to check if they contain relevant data.
* **O(1) Data Pruning:** By instantly checking the pre-computed upper and lower column bounds inside the manifest, query engines skip irrelevant data files entirely without touching cloud object storage.
* **Hidden Partitioning:** Because partition data is stored textually inside the manifest rather than dictated by a physical folder path (like /year=2026/), you can alter your partitioning strategy layout over time without rebuilding historical data.
### 2.2 Manifest List
A manifest list is an Avro-formatted metadata file that represents a snapshot (a specific version) of an Iceberg table at a given point in time. It sits exactly one layer above individual manifest files in the Iceberg metadata hierarchy.  
Structurally, a manifest list consists of an array of structs. Each individual struct is dedicated to tracking exactly one manifest file and contains high-level summary metadata:

* File Location: The exact physical URI path to find that specific manifest file.
* Partition Membership: The specific partitions to which the data belongs.
* Partition Column Bounds: The precise upper and lower bounds (minimum and maximum values) of the partition columns for all the data files tracked inside that manifest.
* File Counts: The number of data files added, deleted, or left unchanged.

**Key Functions**

* **Top-Level Partition Pruning**: Query engines evaluate the upper and lower partition bounds stored inside this array of structs first. This allows the engine to instantly skip entire manifest files without ever opening them, drastically speeding up query planning.
* **Instant Time Travel**: Because every snapshot of an Iceberg table points to its own dedicated manifest list, query engines can instantly isolate and reconstruct what the table looked like at any historical point in time.
* **O(1) Planning Scalability**: By consolidating multiple manifest file summaries into a single list, Iceberg replaces expensive cloud storage file listings with fast, lightweight metadata reads.
### 2.3 Metadata Files
