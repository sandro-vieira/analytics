-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Manipulating Tables with Delta Lake
-- MAGIC 
-- MAGIC This notebook provides some of the basic functionality of Delta Lake.
-- MAGIC 
-- MAGIC - Executing standard operations to create and manipulate Delta Lake tables, including:
-- MAGIC   - **`CREATE TABLE`**
-- MAGIC   - **`INSERT INTO`**
-- MAGIC   - **`SELECT FROM`**
-- MAGIC   - **`UPDATE`**
-- MAGIC   - **`DELETE`**
-- MAGIC   - **`MERGE`**
-- MAGIC   - **`DROP TABLE`**
-- MAGIC   
-- MAGIC - Using Python **`assert`** command to run checks over the applied **`SQL`** command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Table
-- MAGIC 
-- MAGIC Creating a table to track our bean collection.
-- MAGIC 
-- MAGIC Creating a managed Delta Lake table named **`beans`** following the schema below:
-- MAGIC 
-- MAGIC | Field Name | Field type |
-- MAGIC | --- | --- |
-- MAGIC | name | STRING |
-- MAGIC | color | STRING |
-- MAGIC | grams | FLOAT |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking the table structure created.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans"), "Table named `beans` does not exist"
-- MAGIC assert spark.table("beans").columns == ["name", "color", "grams", "delicious"], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("beans").dtypes == [("name", "string"), ("color", "string"), ("grams", "float"), ("delicious", "boolean")], "Please make sure the column types are identical to those provided above"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Data
-- MAGIC 
-- MAGIC Inserting three rows into the table.

-- COMMAND ----------

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inserting the additional records executing as a single transaction.

-- COMMAND ----------

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm the data is in the proper state.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").count() == 6, "The table should have 6 records"
-- MAGIC assert spark.conf.get("spark.databricks.delta.lastCommitVersionInSession") == "2", "Only 3 commits should have been made to the table"
-- MAGIC assert set(row["name"] for row in spark.table("beans").select("name").collect()) == {'beanbag chair', 'black', 'green', 'jelly', 'lentils', 'pinto'}, "Make sure you have not modified the data provided"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Update Records
-- MAGIC 
-- MAGIC Run the following cell to update this record.

-- COMMAND ----------

UPDATE beans
   SET delicious = true
 WHERE name = "jelly"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Run the cell below to confirm this has completed properly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("name='jelly'").count() == 1, "There should only be 1 entry for jelly beans"
-- MAGIC row = spark.table("beans").filter("name='jelly'").first()
-- MAGIC assert row["color"] == "rainbow", "The jelly bean should be labeled as the color rainbow"
-- MAGIC assert row["grams"] == 42.5, "Make sure you correctly specified the `grams` as 42.5"
-- MAGIC assert row["delicious"] == True, "The jelly bean is a delicious bean"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delete Records
-- MAGIC 
-- MAGIC Execute a query to drop all beans that are not delicious.

-- COMMAND ----------

DELETE FROM beans
 WHERE delicious = false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the following cell to confirm this operation was successful.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("beans").filter("delicious=true").count() == 5, "There should be 5 delicious beans in your table"
-- MAGIC assert spark.table("beans").filter("name='beanbag chair'").count() == 0, "Make sure your logic deletes non-delicious beans"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using Merge to Upsert Records
-- MAGIC 
-- MAGIC The cell below registers these as a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

SELECT * FROM new_beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the cell below, use the above view to write a merge statement to update and insert new records to your **`beans`** table as one transaction.
-- MAGIC 
-- MAGIC Make sure your logic:
-- MAGIC - Matches beans by name **and** color
-- MAGIC - Updates existing beans by adding the new weight to the existing weight
-- MAGIC - Inserts new beans only if they are delicious

-- COMMAND ----------

MERGE INTO beans b
USING new_beans nb
   ON b.name = nb.name 
  AND b.color = nb.color
 WHEN MATCHED THEN
   UPDATE SET grams = b.grams + nb.grams
 WHEN NOT MATCHED AND nb.delicious = true THEN
   INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to check merge results.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql("DESCRIBE HISTORY beans").selectExpr("max(version)").first()[0]
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").filter(f"version={version}")
-- MAGIC assert last_tx.select("operation").first()[0] == "MERGE", "Transaction should be completed as a merge"
-- MAGIC metrics = last_tx.select("operationMetrics").first()[0]
-- MAGIC assert metrics["numOutputRows"] == "3", "Make sure you only insert delicious beans"
-- MAGIC assert metrics["numTargetRowsUpdated"] == "1", "Make sure you match on name and color"
-- MAGIC assert metrics["numTargetRowsInserted"] == "2", "Make sure you insert newly collected beans"
-- MAGIC assert metrics["numTargetRowsDeleted"] == "0", "No rows should be deleted by this operation"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping Tables
-- MAGIC 
-- MAGIC When working with managed Delta Lake tables, dropping a table results in permanently deleting access to the table and all underlying data files.

-- COMMAND ----------

DROP TABLE beans

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to assert that your table no longer exists.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql("SHOW TABLES LIKE 'beans'").collect() == [], "Confirm that you have dropped the `beans` table from your current database"
