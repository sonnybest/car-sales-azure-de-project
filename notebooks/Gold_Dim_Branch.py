# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databrickspro;
# MAGIC USE SCHEMA gold;
# MAGIC

# COMMAND ----------

df = spark.read.format('parquet')\
    .load("abfss://silver@storageaccountcars.dfs.core.windows.net//carsales")

display(df)

# COMMAND ----------

df.write.mode("overwrite")\
    .saveAsTable("databrickspro.silver.carsales_silver")

# COMMAND ----------

df_src = spark.sql("""
                   SELECT DISTINCT Branch_ID, BranchName
                   FROM databrickspro.silver.carsales_silver
                   """)
display(df_src)


# COMMAND ----------

if spark.catalog.tableExists("databrickspro.gold.DimBranch"):
    df_sink = spark.sql("""SELECT Branch_ID, BranchName, DimBranchKey
                        FROM databrickspro.gold.DimBranch""")
else:
    df_sink = spark.sql("""
                        SELECT Branch_ID, BranchName, 0 AS DimBranchKey
                        FROM databrickspro.silver.carsales_silver
                        WHERE 1 = 0""")
display(df_sink)



# COMMAND ----------

df_join = df_src.join(df_sink, df_src.Branch_ID == df_sink.Branch_ID, how='left')\
    .select(df_src.Branch_ID, df_src.BranchName, df_sink.DimBranchKey)\
    
display(df_join)

# COMMAND ----------

df_new = df_join.where(df_join.DimBranchKey.isNull())\
    .drop('DimBranchKey')
df_old = df_join.where(df_join.DimBranchKey.isNotNull())

# COMMAND ----------

display(df_new)

# COMMAND ----------

display(df_old)

# COMMAND ----------

if spark.catalog.tableExists("databrickspro.gold.DimBranch"):
    max_val = spark.sql("""
                        SELECT MAX(DimBranchKey) AS max_val
                        FROM databrickspro.gold.DimBranch
                        """).collect()[0]['max_val'] + 1
else:
    max_val= 1

# COMMAND ----------

display(max_val)

# COMMAND ----------

df_new = df_new.withColumn("DimBranchKey", lit(max_val) + monotonically_increasing_id())
display(df_new)

# COMMAND ----------

df_final = df_new.union(df_old)
display(df_final)


# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databrickspro.gold.DimBranch"):
    dlt_obj = DeltaTable.forName(spark, "databrickspro.gold.DimBranch")
    dlt_obj.alias("t").merge(
        df_final.alias("s"),
        "t.Branch_ID = s.Branch_ID",
    ).whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_final.write.mode("overwrite")\
        .saveAsTable("databrickspro.gold.DimBranch")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databrickspro.gold.DimBranch
# MAGIC ORDER BY DimBranchKey;

# COMMAND ----------

# spark.sql("DROP TABLE IF EXISTS databrickspro.gold.DimBranch")
# spark.sql("DROP TABLE IF EXISTS databrickspro.gold.DimModel")
# spark.sql("DROP TABLE IF EXISTS databrickspro.gold.DimDate")
# spark.sql("DROP TABLE IF EXISTS databrickspro.gold.DimDealer")