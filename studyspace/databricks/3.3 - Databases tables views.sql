-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databases, Tables, and Views
-- MAGIC 
-- MAGIC ## Objectives
-- MAGIC - Create and explore interactions between relational entities:
-- MAGIC   - Databases
-- MAGIC   - Tables (managed and external)
-- MAGIC   - Views (views, temp views, and global temp views)
-- MAGIC 
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html" target="_blank">Databases and Tables - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables" target="_blank">Managed and Unmanaged Tables</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui" target="_blank">Creating a Table with the UI</a>
-- MAGIC * <a href="https://docs.databricks.com/user-guide/tables.html#create-a-local-table" target="_blank">Create a Local Table</a>
-- MAGIC * <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables" target="_blank">Saving to Persistent Tables</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview of the Data
-- MAGIC 
-- MAGIC The data include multiple entries from a selection of weather stations, including average temperatures recorded in either Fahrenheit or Celsius. The schema for the table:
-- MAGIC 
-- MAGIC |ColumnName  | DataType| Description|
-- MAGIC |------------|---------|------------|
-- MAGIC |NAME        |string   | Station name |
-- MAGIC |STATION     |string   | Unique ID |
-- MAGIC |LATITUDE    |float    | Latitude |
-- MAGIC |LONGITUDE   |float    | Longitude |
-- MAGIC |ELEVATION   |float    | Elevation |
-- MAGIC |DATE        |date     | YYYY-MM-DD |
-- MAGIC |UNIT        |string   | Temperature units |
-- MAGIC |TAVG        |float    | Average temperature |
-- MAGIC 
-- MAGIC This data is stored in the Parquet format.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Preview the data with the query below.

-- COMMAND ----------

SELECT * 
FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Database
-- MAGIC 
-- MAGIC Create a database in the default location using the **`da.db_name`**

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking the new database.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 1, "Database not present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Changing to New Database
-- MAGIC 
-- MAGIC **`USE`** your newly created database.

-- COMMAND ----------

USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW CURRENT DATABASE").first()["namespace"] == DA.db_name, "Not using the correct database"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Managed Table
-- MAGIC Using a CTAS statement to create a managed table named **`weather_managed`**.

-- COMMAND ----------

CREATE OR REPLACE TABLE weather_managed AS
  SELECT * 
    FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking the table creation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create an External Table
-- MAGIC 
-- MAGIC An external table differs from a managed table through specification of a location. Creating an external table called **`weather_external`**.

-- COMMAND ----------

CREATE OR REPLACE TABLE weather_external
LOCATION "${da.paths.working_dir}/lab/external"
AS SELECT * 
     FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking the table creation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_external"), "Table named `weather_external` does not exist"
-- MAGIC assert spark.table("weather_external").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Examine Table Details
-- MAGIC Use the SQL command **`DESCRIBE EXTENDED table_name`** to examine a table.

-- COMMAND ----------

DESCRIBE EXTENDED weather_managed

-- COMMAND ----------

DESCRIBE DETAIL weather_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Running a helper code to extract and compare the table locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def getTableLocation(tableName):
-- MAGIC     return spark.sql(f"DESCRIBE DETAIL {tableName}").select("location").first()[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC managedTablePath = getTableLocation("weather_managed")
-- MAGIC externalTablePath = getTableLocation("weather_external")
-- MAGIC 
-- MAGIC print(f"""The weather_managed table is saved at: 
-- MAGIC 
-- MAGIC     {managedTablePath}
-- MAGIC 
-- MAGIC The weather_external table is saved at:
-- MAGIC 
-- MAGIC     {externalTablePath}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List the contents of directories to confirm that data exists in the locations.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(managedTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(externalTablePath)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dropping Database and All Tables
-- MAGIC Use the **`CASCADE`** keyword.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking dropping command result.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{DA.db_name}'").count() == 0, "Database present"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **This highlights the main differences between managed and external tables.** By default, the files associated with managed tables will be stored to this location on the root DBFS storage linked to the workspace, and will be deleted when a table is dropped.
-- MAGIC 
-- MAGIC Files for external tables will be persisted in the location provided at table creation, preventing users from inadvertently deleting underlying files. **External tables can easily be migrated to other databases or renamed, but these operations with managed tables will require rewriting ALL underlying files.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a Database with a Specified Path

-- COMMAND ----------

CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
USE ${da.db_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the **`weather_managed`** table and print out the location of this table.

-- COMMAND ----------

CREATE OR REPLACE TABLE weather_managed AS
  SELECT *
    FROM parquet.`${da.paths.working_dir}/weather`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC getTableLocation("weather_managed") #using helper created on Cmd 25

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("weather_managed"), "Table named `weather_managed` does not exist"
-- MAGIC assert spark.table("weather_managed").count() == 2559, "Incorrect row count"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views and their Scoping
-- MAGIC 
-- MAGIC Using the provided **`AS`** clause, register:
-- MAGIC - a view named **`celsius`**
-- MAGIC - a temporary view named **`celsius_temp`**
-- MAGIC - a global temp view named **`celsius_global`**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating a view.

-- COMMAND ----------

CREATE OR REPLACE VIEW celsius
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius"), "Table named `celsius` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius'").first()["isTemporary"] == False, "Table is temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating a temporary view.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW celsius_temp
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("celsius_temp"), "Table named `celsius_temp` does not exist"
-- MAGIC assert spark.sql(f"SHOW TABLES").filter(f"tableName == 'celsius_temp'").first()["isTemporary"] == True, "Table is not temporary"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Registering a global temp view.

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW celsius_global
AS (SELECT *
  FROM weather_managed
  WHERE UNIT = "C")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Checking.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("global_temp.celsius_global"), "Global temporary view named `celsius_global` does not exist"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Views will be displayed alongside tables when listing from the catalog.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note the following:
-- MAGIC - The view is associated with the current database. This view will be available to any user that can access this database and will persist between sessions.
-- MAGIC - The temp view is not associated with any database. The temp view is ephemeral and is only accessible in the current SparkSession.
-- MAGIC - The global temp view does not appear in our catalog. **Global temp views will always register to the **`global_temp`** database**. The **`global_temp`** database is ephemeral but tied to the lifetime of the cluster; however, it is only accessible by notebooks attached to the same cluster on which it was created.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean Up
-- MAGIC Dropping the database and all tables to clean up the workspace.

-- COMMAND ----------

DROP DATABASE ${da.db_name} CASCADE
