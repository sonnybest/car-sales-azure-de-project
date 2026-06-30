# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databrickspro.silver.carsales_silver

# COMMAND ----------

df_silver = spark.table("databrickspro.silver.carsales_silver")
display(df_silver)

# COMMAND ----------

# Read all the tables into dataframes
df_date = spark.table("databrickspro.gold.dimdate")
df_model = spark.table("databrickspro.gold.dimmodel")
df_branch = spark.table("databrickspro.gold.dimbranch")
df_dealer = spark.table("databrickspro.gold.dimdealer")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Fact Table

# COMMAND ----------

df_fact = df_silver.join(df_date, df_silver.Date_ID == df_date.Date_ID, "left") \
                  .join(df_model, df_silver.Model_ID == df_model.Model_ID, "left") \
                  .join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, "left") \
                  .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, "left") \
                  .select(df_date.DimDateKey, df_model.DimModelKey, df_branch.DimBranchKey, df_dealer.DimDealerKey, df_silver.Revenue, df_silver.Units_Sold)
display(df_fact)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databrickspro.gold.factcarsales"):
    dlt_obj = DeltaTable.forName(spark, "databrickspro.gold.factcarsales")
    dlt_obj.alias("dlt").merge(
        df_fact.alias("new"),
        "dlt.DimDateKey = new.DimDateKey AND dlt.DimModelKey = new.DimModelKey AND dlt.DimBranchKey = new.DimBranchKey AND dlt.DimDealerKey = new.DimDealerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
  df_fact.write\
      .mode("overwrite")\
      .saveAsTable("databrickspro.gold.factcarsales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databrickspro.gold.factcarsales;