# Databricks notebook source
# MAGIC %md
# MAGIC # Propagating Incremental Updates with Structured Streaming and Delta Lake
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC * Apply your knowledge of structured streaming and Auto Loader to implement a simple multi-hop architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest data
# MAGIC 
# MAGIC Reading data using Auto Loader using its schema inference (use **`customers_checkpoint_path`** to store the schema info). Stream the raw data to a Delta table called **`bronze`**.

# COMMAND ----------

customers_checkpoint_path = f"{DA.paths.checkpoints}/customers"

query = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", customers_checkpoint_path)
              .load("/databricks-datasets/retail-org/customers/")
              .writeStream
              .format("delta")
              .option("checkpointLocation", customers_checkpoint_path)
              .outputMode("append")
              .table("bronze"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to check your work (this is a reference only. You should change it in accordance with your sample).

# COMMAND ----------

assert spark.table("bronze"), "Table named `bronze` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("bronze").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create a streaming temporary view into the bronze table, so that we can perform transforms using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and enhance data
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`bronze_enhanced_temp`** that does the following:
# MAGIC * Skips records with a null **`postcode`** (set to zero)
# MAGIC * Inserts a column called **`receipt_time`** containing a current timestamp
# MAGIC * Inserts a column called **`source_file`** containing the input filename

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM bronze_temp
# MAGIC  WHERE postcode > 0 

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to check your work (this is a reference only. You should change it in accordance with your sample).

# COMMAND ----------

assert spark.table("bronze_enhanced_temp"), "Table named `bronze_enhanced_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'bronze_enhanced_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("bronze_enhanced_temp").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("bronze_enhanced_temp").isStreaming, "Not a streaming table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver table
# MAGIC 
# MAGIC Stream the data from **`bronze_enhanced_temp`** to a table called **`silver`**.

# COMMAND ----------

silver_checkpoint_path = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
              .writeStream
              .format("delta"
              .option("checkpointLocation", silver_checkpoint_path)
              .outputMode("append")
              .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to check your work (this is a reference only. You should change it in accordance with your sample).

# COMMAND ----------

assert spark.table("silver"), "Table named `silver` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'silver'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("silver").dtypes ==  [('customer_id', 'string'), ('tax_id', 'string'), ('tax_code', 'string'), ('customer_name', 'string'), ('state', 'string'), ('city', 'string'), ('postcode', 'string'), ('street', 'string'), ('number', 'string'), ('unit', 'string'), ('region', 'string'), ('district', 'string'), ('lon', 'string'), ('lat', 'string'), ('ship_to_address', 'string'), ('valid_from', 'string'), ('valid_to', 'string'), ('units_purchased', 'string'), ('loyalty_segment', 'string'), ('_rescued_data', 'string'), ('receipt_time', 'timestamp'), ('source_file', 'string')], "Incorrect Schema"
assert spark.table("silver").filter("postcode <= 0").count() == 0, "Null postcodes present"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Creating a streaming temporary view into the silver table, so that we can perform business-level using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold tables
# MAGIC 
# MAGIC Using CTAS syntax, define a new streaming view called **`customer_count_temp`** that counts customers per state.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_temp AS
# MAGIC SELECT state, count(customer_id) AS customer_count
# MAGIC   FROM silver_temp
# MAGIC  GROUP BY state

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to check your work (this is a reference only. You should change it in accordance with your sample).

# COMMAND ----------

assert spark.table("customer_count_temp"), "Table named `customer_count_temp` does not exist"
assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'customer_count_temp'").first()["isTemporary"] == True, "Table is not temporary"
assert spark.table("customer_count_temp").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, stream the data from the **`customer_count_temp`** view to a Delta table called **`gold_customer_count_by_state`**.

# COMMAND ----------

customers_count_checkpoint_path = f"{DA.paths.checkpoints}/customers_counts"

query = (spark
  .table("customer_count_temp")
  .writeStream
  .format("delta")
  .option("checkpointLocation", customers_count_checkpoint_path)
  .outputMode("complete")
  .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to check your work (this is a reference only. You should change it in accordance with your sample).

# COMMAND ----------

assert spark.table("gold_customer_count_by_state"), "Table named `gold_customer_count_by_state` does not exist"
assert spark.sql(f"show tables").filter(f"tableName == 'gold_customer_count_by_state'").first()["isTemporary"] == False, "Table is temporary"
assert spark.table("gold_customer_count_by_state").dtypes ==  [('state', 'string'), ('customer_count', 'bigint')], "Incorrect Schema"
assert spark.table("gold_customer_count_by_state").count() == 51, "Incorrect number of rows" 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the results
# MAGIC 
# MAGIC Query the **`gold_customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state
