# Architecture
## 1. Data Layer
The Apache Iceberg data layer constitutes the physical foundation of a table, storing actual data in formats like Parquet, ORC, or Avro alongside delete files for row-level mutations. This layer relies on scalable infrastructure such as cloud object storage or HDFS
### Data Files
The physical data layer of an Apache Iceberg table relies on file-format agnosticism, natively supporting Parquet, ORC, and Avro. This neutrality accommodates legacy organizational data, fits varying workload demands (e.g., Avro for streaming vs. Parquet for OLAP), and future-proofs infrastructure against changing industry standards.
#### Why Parquet Dominates the Data Layer
While Iceberg is flexible, Apache Parquet is the de facto industry standard for production data lakes due to its highly efficient columnar structure:
* **High Parallelism**: A single Parquet file can be split multiple ways, allowing different compute threads to read sections of the file simultaneously.
* **Vectorized Data Skipping**: It stores granular column-level statistics (min/max values, null counts) at multiple split points, letting engines skip irrelevant data entirely.
* **Superior Compression**: Grouping similar column data types together enables dense algorithmic compression, drastically reducing storage costs and boosting read throughput.
### Delete Files
Because data lake storage is immutable, rows cannot be edited in place. To handle updates and deletions, Apache Iceberg (v2 format) introduces Delete Files to enable the Merge-on-Read (MoR) strategy. Instead of rewriting an entire data file, MoR writes a separate, compact delete file that query engines merge with the base data file at runtime.   

There are two types of Delete Files
#### Positional Delete Files
#### Equality Delete Files
