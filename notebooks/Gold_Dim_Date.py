# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales = spark.read.format("parquet")\
    .load("abfss://silver@storageaccountcars.dfs.core.windows.net/carsales")

# COMMAND ----------

df_sales.write.mode("overwrite")\
    .saveAsTable("databrickspro.silver.carsales_silver")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Flag to Determine Initial or Incremental Run

# COMMAND ----------

dbutils.widgets.text("incremental_flag", "0")

# COMMAND ----------

incremental_flag = int(dbutils.widgets.get("incremental_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  CREATING DIMESION TABLES

# COMMAND ----------

df_src = spark.sql("""
          SELECT DISTINCT Date_ID
          FROM parquet.`abfss://silver@storageaccountcars.dfs.core.windows.net/carsales`""")
display(df_src)

# COMMAND ----------

if not spark.catalog.tableExists("databrickspro.gold.DimDate"):
    df_sink = spark.sql("""SELECT 0 DimDateKey, 0 Date_ID
          FROM databrickspro.silver.carsales_silver
          WHERE 1 = 0
          """)
else:
    df_sink = spark.sql("""SELECT DimDateKey, Date_ID
          FROM databrickspro.gold.DimDate
          """)
display(df_sink)

# COMMAND ----------

df_join = df_src.join(df_sink, df_src.Date_ID == df_sink.Date_ID, "left")\
    .select(df_src.Date_ID, df_sink.DimDateKey)

display(df_join)

# COMMAND ----------

# Separate old records from new records
df_old = df_join.where(df_join.DimDateKey.isNotNull())
df_new = df_join.where(df_join.DimDateKey.isNull())\
    .drop("DimDateKey")

# COMMAND ----------

display(df_old)

# COMMAND ----------

display(df_new)

# COMMAND ----------

# Get the max surrogate key from existing table
if not spark.catalog.tableExists("databrickspro.gold.DimDate"):
    max_val = 1
else:
    max_val = spark.sql("""SELECT MAX(DimDateKey) AS max_val
          FROM databrickspro.gold.DimDate
          """).collect()[0]['max_val'] + 1

df_new = df_new.withColumn("DimDateKey", max_val + monotonically_increasing_id())
print(max_val)


# COMMAND ----------

display(df_new)

# COMMAND ----------

display(df_old)

# COMMAND ----------

# Combine old and new records
df_final = df_new.unionByName(df_old)
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD TYPE 1 UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Cell 26
if spark.catalog.tableExists("databrickspro.gold.DimDate"):
    dlt_obj = DeltaTable.forName(spark, "databrickspro.gold.DimDate")
    dlt_obj.alias("t").merge(df_final.alias("s"), "t.Date_ID = s.Date_ID")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.mode("overwrite")\
        .saveAsTable("databrickspro.gold.DimDate")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE databrickspro.gold.dimmodel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databrickspro.gold.dimdate
# MAGIC ORDER BY DimDateKey

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS databrickspro.gold.DimModel;