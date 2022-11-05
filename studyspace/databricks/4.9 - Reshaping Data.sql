-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Reshaping Data
-- MAGIC 
-- MAGIC ## Objectives
-- MAGIC - Pivot and join tables
-- MAGIC - Higher order functions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshape Datasets to Create Click Paths
-- MAGIC This operation will join data from your **`events`** and **`transactions`** tables in order to create a record of all actions a user took on the site and what their final order looked like.
-- MAGIC 
-- MAGIC The **`clickpaths`** table should contain all the fields from your **`transactions`** table, as well as a count of every **`event_name`** in its own column. Each user that completed a purchase should have a single row in the final table. Let's start by pivoting the **`events`** table to get counts for each **`event_name`**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Pivot **`events`** to count actions for each user
-- MAGIC We want to aggregate the number of times each user performed a specific event, specified in the **`event_name`** column. To do this, group by **`user`** and pivot on **`event_name`** to provide a count of every event type in its own column, resulting in the schema below.
-- MAGIC 
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | pillows | BIGINT |
-- MAGIC | login | BIGINT |
-- MAGIC | main | BIGINT |
-- MAGIC | careers | BIGINT |
-- MAGIC | guest | BIGINT |
-- MAGIC | faq | BIGINT |
-- MAGIC | down | BIGINT |
-- MAGIC | warranty | BIGINT |
-- MAGIC | finalize | BIGINT |
-- MAGIC | register | BIGINT |
-- MAGIC | shipping_info | BIGINT |
-- MAGIC | checkout | BIGINT |
-- MAGIC | mattresses | BIGINT |
-- MAGIC | add_item | BIGINT |
-- MAGIC | press | BIGINT |
-- MAGIC | email_coupon | BIGINT |
-- MAGIC | cc_info | BIGINT |
-- MAGIC | foam | BIGINT |
-- MAGIC | reviews | BIGINT |
-- MAGIC | original | BIGINT |
-- MAGIC | delivery | BIGINT |
-- MAGIC | premium | BIGINT |
-- MAGIC 
-- MAGIC A list of the event names are provided below.

-- COMMAND ----------

CREATE OR REPLACE VIEW events_pivot AS
SELECT * FROM (
  SELECT user_id AS user, event_name
    FROM events
) PIVOT (count(*) FOR event_name IN 
("cart", "pillows", "login", "main", "careers", "guest", "faq", "down", "warranty", "finalize", 
"register", "shipping_info", "checkout", "mattresses", "add_item", "press", "email_coupon", 
"cc_info", "foam", "reviews", "original", "delivery", "premium"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE**: Using Python to run checks occasionally throughout the queries. The helper functions below will return an error with a message when the query was not run as expected. No output means that you have completed this step.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, column_names, num_rows):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert spark.table(table_name).columns == column_names, "Please name the columns in the order provided above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Adjuste and run the code below, in accordance with your needes, to confirm the view was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC event_columns = ['user', 'cart', 'pillows', 'login', 'main', 'careers', 'guest', 'faq', 'down', 'warranty', 'finalize', 'register', 'shipping_info', 'checkout', 'mattresses', 'add_item', 'press', 'email_coupon', 'cc_info', 'foam', 'reviews', 'original', 'delivery', 'premium']
-- MAGIC check_table_results("events_pivot", event_columns, 204586)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Join event counts and transactions for all users
-- MAGIC 
-- MAGIC Join **`events_pivot`** with **`transactions`** to create the table **`clickpaths`**. This table should have the same event name columns from the **`events_pivot`** table created above, followed by columns from the **`transactions`** table, as shown below.
-- MAGIC 
-- MAGIC | field | type | 
-- MAGIC | --- | --- | 
-- MAGIC | user | STRING |
-- MAGIC | cart | BIGINT |
-- MAGIC | ... | ... |
-- MAGIC | user_id | STRING |
-- MAGIC | order_id | BIGINT |
-- MAGIC | transaction_timestamp | BIGINT |
-- MAGIC | total_item_quantity | BIGINT |
-- MAGIC | purchase_revenue_in_usd | DOUBLE |
-- MAGIC | unique_items | BIGINT |
-- MAGIC | P_FOAM_K | BIGINT |
-- MAGIC | M_STAN_Q | BIGINT |
-- MAGIC | P_FOAM_S | BIGINT |
-- MAGIC | M_PREM_Q | BIGINT |
-- MAGIC | M_STAN_F | BIGINT |
-- MAGIC | M_STAN_T | BIGINT |
-- MAGIC | M_PREM_K | BIGINT |
-- MAGIC | M_PREM_F | BIGINT |
-- MAGIC | M_STAN_K | BIGINT |
-- MAGIC | M_PREM_T | BIGINT |
-- MAGIC | P_DOWN_S | BIGINT |
-- MAGIC | P_DOWN_K | BIGINT |

-- COMMAND ----------

CREATE OR REPLACE VIEW clickpaths AS
SELECT *
  FROM events_pivot e
  JOIN transactions t
    ON e.user = t.user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clickpath_columns = event_columns + ['user_id', 'order_id', 'transaction_timestamp', 'total_item_quantity', 'purchase_revenue_in_usd', 'unique_items', 'P_FOAM_K', 'M_STAN_Q', 'P_FOAM_S', 'M_PREM_Q', 'M_STAN_F', 'M_STAN_T', 'M_PREM_K', 'M_PREM_F', 'M_STAN_K', 'M_PREM_T', 'P_DOWN_S', 'P_DOWN_K']
-- MAGIC check_table_results("clickpaths", clickpath_columns, 9085)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flag Types of Products Purchased
-- MAGIC Here, you'll use the higher order function **`EXISTS`** to create boolean columns **`mattress`** and **`pillow`** that indicate whether the item purchased was a mattress or pillow product.
-- MAGIC 
-- MAGIC For example, if **`item_name`** from the **`items`** column ends with the string **`"Mattress"`**, the column value for **`mattress`** should be **`true`** and the value for **`pillow`** should be **`false`**. Here are a few examples of items and the resulting values.
-- MAGIC 
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC 
-- MAGIC See documentation for the <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> function.  
-- MAGIC You can use the condition expression **`item_name LIKE "%Mattress"`** to check whether the string **`item_name`** ends with the word "Mattress".

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_product_flags AS
  SELECT items,
         EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
         EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
    FROM sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to confirm the table was created correctly.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", ['items', 'mattress', 'pillow'], 10539)
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 10015, 'num_pillow': 1386}, "There should be 10015 rows where mattress is true, and 1386 where pillow is true"
