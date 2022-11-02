-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Advanced Delta Lake Features
-- MAGIC 
-- MAGIC **Note** that while some of the keywords used here aren't part of standard ANSI SQL, all Delta Lake operations can be run on Databricks using SQL
-- MAGIC 
-- MAGIC * Use **`OPTIMIZE`** to compact small files
-- MAGIC * Use **`ZORDER`** to index tables
-- MAGIC * Clean up stale data files with **`VACUUM`**
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a Delta Table with History
-- MAGIC 
-- MAGIC The cell below condenses all the transactions into a single cell. (Except **`DROP TABLE`**!)

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
   SET value = value + 1
 WHERE name LIKE "T%";

DELETE FROM students 
 WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students s
USING updates u
   ON s.id=u.id
 WHEN MATCHED AND u.type = "update"
   THEN UPDATE SET *
 WHEN MATCHED AND u.type = "delete"
   THEN DELETE
 WHEN NOT MATCHED AND u.type = "insert"
   THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Examine Table Details
-- MAGIC 
-- MAGIC Databricks uses a Hive metastore by default to register databases, tables, and views.
-- MAGIC 
-- MAGIC Using **`DESCRIBE EXTENDED`** allows to see important metadata about tables.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **`DESCRIBE DETAIL`** is another command that allows to explore table metadata.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore Delta Lake Files
-- MAGIC 
-- MAGIC It is possible to see the files backing in Delta Lake table by using a Databricks Utilities function.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students")) #/tablename

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The directory contains a number of Parquet data files and a directory named **`_delta_log`**.
-- MAGIC 
-- MAGIC Records in Delta Lake tables are stored as data in Parquet files.
-- MAGIC 
-- MAGIC Transactions to Delta Lake tables are recorded in the **`_delta_log`**.
-- MAGIC 
-- MAGIC Peeking inside the **`_delta_log`** to see more.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To have a look at the transaction log run the command below, replacing the json file name.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Compacting Small Files and Indexing
-- MAGIC 
-- MAGIC Small files can occur for a variety of reasons.
-- MAGIC 
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC 
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC 
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

OPTIMIZE students
  ZORDER BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reviewing Delta Lake Transactions
-- MAGIC 
-- MAGIC Because all changes to the Delta Lake table are stored in the transaction log, it is possible to easily review the <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The command **`OPTIMIZE`** creats another version of the table, setting it as the most current version.
-- MAGIC 
-- MAGIC Selecting onto a table using version is like a time travel.

-- COMMAND ----------

SELECT * 
  FROM students VERSION AS OF 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What's important to note about time travel is that we're not recreating a previous state of the table by undoing transactions against our current version; rather, we're just querying all those data files that were indicated as valid as of the specified version.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Rollback Versions
-- MAGIC 
-- MAGIC Suppose you're typing up query to manually delete or update some records from a table and you accidentally execute this query it is possible to rollback to a previous version of the table or to the version you would like to.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 -- use the version number that you need restore to.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that a **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> is recorded as a transaction; you won't be able to completely hide the fact that you accidentally deleted all the records in the table, but you will be able to undo the operation and bring your table back to a desired state.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleaning Up Stale Files
-- MAGIC 
-- MAGIC Databricks will automatically clean up stale files in Delta Lake tables.
-- MAGIC 
-- MAGIC While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive (and can lead to compliance issues if PII is present).
-- MAGIC 
-- MAGIC To manually purge old data files, this can be performed with the **`VACUUM`** operation.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS --Will raise a error

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By default, **`VACUUM`** will prevent you from deleting files less than 7 days old.
-- MAGIC 
-- MAGIC In  order to overpass this prevention:
-- MAGIC 1. Turn off a check to prevent premature deletion of data files
-- MAGIC 1. Make sure that logging of **`VACUUM`** commands is enabled
-- MAGIC 1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted
-- MAGIC 
-- MAGIC **Note:** This is for test/development purpose and **is not typically done in production**.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By running **`VACUUM`** and deleting the files, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))
