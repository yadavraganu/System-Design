### 1. Watermarking (Date or ID based)
This method tracks the "high watermark"—the last successfully processed point in your data.  
- **Timestamp-based:** The pipeline fetches data where the last_modified date is greater than the last run’s timestamp.  
- **Numeric-based:** The pipeline filters data using an auto-incrementing ID or transaction number (e.g., fetching rows with an ID higher than the last saved ID).  
- **Pros & Cons:** Highly efficient, but can miss deleted records unless combined with another method.  

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
