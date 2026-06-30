# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales = spark.read.format("parquet")\
    .load("abfss://bronze@storageaccountcars.dfs.core.windows.net/rawdata")

# COMMAND ----------

display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforming Data with Pyspark Functions

# COMMAND ----------

df_sales = df_sales.withColumn("Model_Category", split("Model_ID", "-").getItem(0))
display(df_sales)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Add the date column to the dataframe
df_sales = df_sales.withColumn("date", expr("try_cast(concat(Year, '-', lpad(Month, 2, '0'), '-', lpad(Day, 2, '0')) as date)"))
display(df_sales)

# COMMAND ----------

# Find out the no. of units sold every year in each branch
df_sales.groupBy("Year", "BranchName")\
    .agg(sum("Units_Sold").alias("Total_Units"))\
    .orderBy(desc("Total_Units"))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Writing

# COMMAND ----------

df_sales.write.mode("overwrite") \
    .format("parquet")\
    .save("abfss://silver@storageaccountcars.dfs.core.windows.net/carsales")

# COMMAND ----------

df_sales.write.mode("overwrite") \
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("databrickspro.silver.carsales_silver")

# COMMAND ----------

spark.table("databrickspro.silver.carsales_silver").display()