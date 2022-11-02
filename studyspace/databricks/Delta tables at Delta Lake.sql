-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Creating Delta Table
-- MAGIC 
-- MAGIC In this notebook, there are basic commands for manipulation of data and tables with SQL on Databricks.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Going back and run the cell above again...it will error out! This is expected - because the table exists already.
-- MAGIC Adding in an additional argument, **`IF NOT EXISTS`** which checks if the table exists. This will overcome the error.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Inserting data

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the cell above, we completed three separate **`INSERT`** statements. Each of these is processed as a separate transaction with its own ACID guarantees.

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the cell above the three records are inserted as one statment.
-- MAGIC Databricks doesn't have a **`COMMIT`** keyword; transactions run as soon as they're executed, and commit as they succeed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying a Delta Table
-- MAGIC 
-- MAGIC Querying a Delta Lake table is as easy as using a standard **`SELECT`** statement.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Records
-- MAGIC 
-- MAGIC Updating records provides atomic guarantees.

-- COMMAND ----------

UPDATE students 
   SET value = value + 1
 WHERE name LIKE "T%"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting Records
-- MAGIC 
-- MAGIC Deleting records provides atomic garantees.
-- MAGIC 
-- MAGIC A **`DELETE`** statement can remove one or many records, but will always result in a single transaction.

-- COMMAND ----------

DELETE FROM students 
 WHERE value > 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using Merge
-- MAGIC 
-- MAGIC Some SQL systems have the concept of an upsert, which allows updates, inserts, and other data manipulations to be run as a single command.
-- MAGIC 
-- MAGIC Databricks uses the **`MERGE`** keyword to perform this operation.

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dropping a Table
-- MAGIC 
-- MAGIC Permanently delete data in the lakehouse using a **`DROP TABLE`** command.

-- COMMAND ----------

DROP TABLE students
