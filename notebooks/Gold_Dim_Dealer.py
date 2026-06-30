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
          SELECT DISTINCT Dealer_ID, DealerName 
          FROM parquet.`abfss://silver@storageaccountcars.dfs.core.windows.net/carsales`""")
display(df_src)

# COMMAND ----------

if not spark.catalog.tableExists("databrickspro.gold.DimDealer"):
    df_sink = spark.sql("""SELECT 0 DimDealerKey, 0 Dealer_ID, 0 DealerName
          FROM databrickspro.silver.carsales_silver
          WHERE 1 = 0
          """)
else:
    df_sink = spark.sql("""SELECT DimDealerKey, Dealer_ID, DealerName
          FROM databrickspro.gold.DimDealer
          """)
display(df_sink)

# COMMAND ----------

df_join = df_src.join(df_sink, df_src.Dealer_ID == df_sink.Dealer_ID, "left")\
    .select(df_src.Dealer_ID, df_src.DealerName, df_sink.DimDealerKey)

display(df_join)

# COMMAND ----------

# Separate old records from new records
df_old = df_join.where(df_join.DimDealerKey.isNotNull())
df_new = df_join.where(df_join.DimDealerKey.isNull())\
    .drop("DimDealerKey")

# COMMAND ----------

display(df_old)

# COMMAND ----------

display(df_new)

# COMMAND ----------

# Get the max surrogate key from existing table
if not spark.catalog.tableExists("databrickspro.gold.DimDealer"):
    max_val = 1
else:
    max_val = spark.sql("""SELECT MAX(DimDealerKey) AS max_val
          FROM databrickspro.gold.DimDealer
          """).collect()[0]['max_val'] + 1

df_new = df_new.withColumn("DimDealerKey", max_val + monotonically_increasing_id())
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
if spark.catalog.tableExists("databrickspro.gold.DimDealer"):
    dlt_obj = DeltaTable.forName(spark, "databrickspro.gold.DimDealer")
    dlt_obj.alias("t").merge(df_final.alias("s"), "t.Dealer_ID = s.Dealer_ID")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_final.write.mode("overwrite")\
        .saveAsTable("databrickspro.gold.DimDealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE databrickspro.gold.dimmodel

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databrickspro.gold.dimdealer
# MAGIC ORDER BY DimDealerKey

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS databrickspro.gold.DimModel;