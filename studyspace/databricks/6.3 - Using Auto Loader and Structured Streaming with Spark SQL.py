# Databricks notebook source
# MAGIC %md
# MAGIC # Using Auto Loader and Structured Streaming with Spark SQL
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC * Ingest data using Auto Loader
# MAGIC * Aggregate streaming data
# MAGIC * Stream data to a Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Streaming Read
# MAGIC 
# MAGIC Reading data using <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Create a streaming temporary view called **`customers_raw_temp`**.

# COMMAND ----------

customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

(spark
  .readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", customers_checkpoint_path)
  .load("/databricks-datasets/retail-org/customers/")
  .createOrReplaceTempView("customers_raw_temp"))

# COMMAND ----------

from pyspark.sql import Row
assert Row(tableName="customers_raw_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customers_raw_temp").dtypes ==  [('customer_id', 'string'),
 ('tax_id', 'string'),
 ('tax_code', 'string'),
 ('customer_name', 'string'),
 ('state', 'string'),
 ('city', 'string'),
 ('postcode', 'string'),
 ('street', 'string'),
 ('number', 'string'),
 ('unit', 'string'),
 ('region', 'string'),
 ('district', 'string'),
 ('lon', 'string'),
 ('lat', 'string'),
 ('ship_to_address', 'string'),
 ('valid_from', 'string'),
 ('valid_to', 'string'),
 ('units_purchased', 'string'),
 ('loyalty_segment', 'string'),
 ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a streaming aggregation
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_by_state_temp`** that counts the number of customers per **`state`**, in a field called **`customer_count`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC SELECT
# MAGIC   state,
# MAGIC   COUNT(customer_id) AS customer_count
# MAGIC  FROM customers_raw_temp
# MAGIC GROUP BY state

# COMMAND ----------

assert Row(tableName="customer_count_by_state_temp", isTemporary=True) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state_temp").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write aggregated data to a Delta table
# MAGIC 
# MAGIC Stream data from the **`customer_count_by_state_temp`** view to a Delta table called **`customer_count_by_state`**.

# COMMAND ----------

customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_count"

query = (spark.table("customer_count_by_state_temp")
         .writeStream
         .format("delta")
         .option("checkpointLocation", customers_count_checkpoint_path)
         .outputMode("complete")
         .table("customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

assert Row(tableName="customer_count_by_state", isTemporary=False) in spark.sql("show tables").select("tableName", "isTemporary").collect(), "Table not present or not temporary"
assert spark.table("customer_count_by_state").dtypes == [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`customer_count_by_state`** table (this will not be a streaming query).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer_count_by_state
