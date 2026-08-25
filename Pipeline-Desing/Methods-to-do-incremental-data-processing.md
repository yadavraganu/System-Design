## 1. Watermarking
Watermark-based incremental loading is a data processing method that copies only the new or changed data from a source system to a target destination, based on a specific tracking point called a "watermark". Instead of reloading the entire dataset every time, the pipeline checks the watermark (such as a "last modified timestamp" or a sequential ID) and extracts only records that have a value greater than the last recorded watermark.
### How It Works
- **Read the Old Watermark:** The pipeline checks a control table or file to find the highest timestamp or ID loaded in the previous run.
- **Read the Source Data:** The pipeline queries the source database or API, asking for only the records created or updated after the old watermark.
- **Load to Target:** The new records are copied or merged into the destination system.
- **Update the Watermark:** The pipeline records the highest timestamp or ID from this new batch and saves it as the new watermark for the next run.
### Pros
- **Fast and Efficient:** Loading only new data reduces network traffic, processing time, and compute costs.
- **Simple Setup:** It is much easier to build and maintain than complex Change Data Capture (CDC) systems.
- **Low Impact on Source:** It runs lightweight queries instead of scanning the entire source database.
- **Great for Append-Only Data:** It perfectly handles data that is only ever added to (like logs or daily sales records).
### Cons
- **Cannot Track Deletes:** If a row is deleted at the source, the watermark method does not recognize the deletion, so the row remains at the target.
- **Requires Source Support**: It needs a reliable column in the source system that automatically updates whenever a change happens.
- **Complexity with Soft Deletes:** To capture deleted data, the source system must use "soft deletes" (e.g., updating a status column to "deleted" rather than removing the row), which requires extra management.

---

### 2. Change Data Capture (CDC)
CDC actively listens to database logs to capture every insert, update, and delete operation as it happens.
- **Log-based CDC:** Reads database transaction logs (using tools like Debezium) to find exact row changes.
- **Native CDC:** Many modern databases have built-in CDC features that automatically log row changes and provide metadata.
- **Pros & Cons:** The most reliable way to capture all changes (including deletes) with low impact on the source system.

### 3. Upsert (Merge)
Used when your data contains both new rows and updates to existing rows.
- **How it works:** New or changed records are compared to the target table using a unique identifier (like customer_id).
- The pipeline then updates existing records and inserts new ones in a single atomic transaction.
- **When to use:** Crucial for maintaining the latest snapshot of a record without duplicating data.

### 4. Partition Overwrites & Append
This pattern is commonly used for immutable data (data that never changes, like logs or clickstreams).
- **Append:** New data is simply added to the end of the destination table.
- **Overwrite Partitions:** Replaces entire data partitions (e.g., rewriting just the data for yesterday's folder) rather than reloading the entire database.
- **When to use:** Ideal for cloud object storage, data lakes, and append-only event streams
