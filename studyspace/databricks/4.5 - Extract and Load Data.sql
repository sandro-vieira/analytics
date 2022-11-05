-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Extract and Load Data
-- MAGIC 
-- MAGIC Extract and load raw data from JSON files into a Delta table.
-- MAGIC 
-- MAGIC ## Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Create an external table to extract data from JSON files
-- MAGIC - Create an empty Delta table with a provided schema
-- MAGIC - Insert records from an existing table into a Delta table
-- MAGIC - Use a CTAS statement to create a Delta table from files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC Sample of raw data written as JSON files. 
-- MAGIC 
-- MAGIC The schema for the table:
-- MAGIC 
-- MAGIC | field  | type | description |
-- MAGIC | ------ | ---- | ----------- |
-- MAGIC | key    | BINARY | The **`user_id`** field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information |
-- MAGIC | offset | LONG | This is a unique value, monotonically increasing for each partition |
-- MAGIC | partition | INTEGER | Our current Kafka implementation uses only 2 partitions (0 and 1) |
-- MAGIC | timestamp | LONG    | This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition |
-- MAGIC | topic | STRING | While the Kafka service hosts multiple topics, only those records from the **`clickstream`** topic are included here |
-- MAGIC | value | BINARY | This is the full data payload (to be discussed later), sent as JSON |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract Raw Events From JSON Files
-- MAGIC Extract the JSON data using the correct schema.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset BIGINT, partition INTEGER, timestamp BIGINT, topic STRING, value BINARY)
 USING JSON
OPTIONS (path = "${da.paths.datasets}/raw/events-kafka/") --Use your path, where the Json files are stored.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE**: Using Python to run checks. The following cell will return an error with a message on what needs to change if you have not followed instructions. No output from cell execution means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC 
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Raw Events Into Delta Table
-- MAGIC Creating an empty managed Delta table named **`events_raw`** using the same schema.

-- COMMAND ----------

CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Using Python to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw"), "Table named `events_raw` does not exist"
-- MAGIC assert spark.table("events_raw").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_raw").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC assert spark.table("events_raw").count() == 0, "The table should have 0 records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inserting the JSON records from the **`events_json`** table into the new **`events_raw`** Delta table.

-- COMMAND ----------

INSERT INTO events_raw
  SELECT * FROM events_json;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Review the table contents to ensure data was written as expected.

-- COMMAND ----------

SELECT * FROM events_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirming the data has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_raw").count() == 2252, "The table should have 2252 records"
-- MAGIC assert set(row['timestamp'] for row in spark.table("events_raw").select("timestamp").limit(5).collect()) == {1593880885085, 1593880892303, 1593880889174, 1593880886106, 1593880889725}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Delta Table from a Query
-- MAGIC Loading a small lookup table that provides product details.
-- MAGIC Using a CTAS statement to create a managed Delta table named **`item_lookup`** that extracts data from the parquet files.

-- COMMAND ----------

CREATE OR REPLACE TABLE item_lookup AS
  SELECT * FROM parquet.`${da.paths.datasets}/raw/item-lookup`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Confirming the lookup table has been loaded correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("item_lookup").count() == 12, "The table should have 12 records"
-- MAGIC assert set(row['item_id'] for row in spark.table("item_lookup").select("item_id").limit(5).collect()) == {'M_PREM_F', 'M_PREM_K', 'M_PREM_Q', 'M_PREM_T', 'M_STAN_F'}, "Make sure you have not modified the data provided"
