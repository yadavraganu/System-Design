## 1. Append Mode (Default)
Append Mode writes only the brand-new records added to the result table since the last trigger.
```python
df.writeStream
  .format("parquet")
  .outputMode("append")
  .option("path", "path/to/destination")
  .option("checkpointLocation", "path/to/checkpoint")
  .start()
```

* **How it works**: Spark processes incoming data and appends it to the sink. It assumes that once data is written, it will never change or update.
* **Limitations**: You cannot use it with aggregations (like .groupBy()) unless you define a watermark (a time threshold to drop old data). Without a watermark, Spark cannot know if more data will arrive for a group, so it can never finalize and "append" the row.

## 2. Update Mode
Update Mode writes only the rows that were modified or newly added since the last trigger.

```python
df.groupBy("user_id").count()
  .writeStream
  .format("console")
  .outputMode("update")
  .start()
```
* **How it works**: If a user's count changes from 5 to 6 in a micro-batch, only that user's updated row is pushed to the sink. If another user's count stays at 5, their row is skipped entirely.
* **Limitations**: It is highly efficient, but not all file formats support it. For example, traditional file sinks like Parquet or CSV do not natively support updating specific rows. It works best with sinks like Key-Value stores, Databases, Delta Lake, or Kafka.

## 3. Complete Mode
Complete Mode rewrites the entire result table to the sink every time a trigger happens.

```python
df.groupBy("category")
  .sum("sales")
  .writeStream
  .format("console")
  .outputMode("complete")
  .start()
```
* **How it works**: Every time new data arrives, Spark recalculates the entire aggregation from the beginning of time and dumps the whole table to the destination.
* **Limitations**: You must use an aggregation with this mode. It also suffers from a severe scalability issue: because the entire table is rewritten every time, memory and storage usage will grow continuously. If your dataset is huge, your streaming application will eventually run out of memory.
