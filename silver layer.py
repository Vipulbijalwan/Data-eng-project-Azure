# Databricks notebook source
# MAGIC %md
# MAGIC ## Connecting Data Lake with DataBricks

# COMMAND ----------

# DBTITLE 1,DATA ACCESS USING APP

spark.conf.set("fs.azure.account.auth.type.companystorages.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.companystorages.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.companystorages.dfs.core.windows.net", "fc90d2a2-4f8d-4c78-a0f5-280fbddedb82")
spark.conf.set("fs.azure.account.oauth2.client.secret.companystorages.dfs.core.windows.net", "cUE8Q~FSGpsOAG2fAz28DX6GE_4ocZrtr0Z8kaw4")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.companystorages.dfs.core.windows.net", "https://login.microsoftonline.com/03b17786-6d87-4826-b8a4-87956dfa4ad5/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA LOADING

# COMMAND ----------

df_cal=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cust=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_pro_cat=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_pro_cat.display()

# COMMAND ----------

df_pro=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_ret=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_ret.display()


# COMMAND ----------

df_sale=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_sale.display()

# COMMAND ----------

df_terr=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_pro_sub=spark.read.format('csv')\
    .option("header",True)\
        .option("inferSchema",True)\
            .load('abfss://bronze@companystorages.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

df_pro_sub.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation
# MAGIC

# COMMAND ----------

df_cal.display()


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Calander

# COMMAND ----------

df_cal=df_cal.withColumn('Month',month(col('date')))
df_cal=df_cal.withColumn("Year",year(col('date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC Customers

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust=df_cust.withColumn("FullName",concat(col('FirstName'),lit(' '),col('LastName')))

# COMMAND ----------

df_cust.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Custmers")

# COMMAND ----------

# MAGIC %md
# MAGIC Subcat
# MAGIC

# COMMAND ----------

df_pro_sub.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro=df_pro.withColumn("ProductSKU",split(col('ProductSKU'),"-").getItem(0))

# COMMAND ----------

df_pro.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Product")

# COMMAND ----------

# MAGIC %md
# MAGIC Return

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_terr.write.format("parquet")\
    .mode("append")\
        .save("abfss://silver@companystorages.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC Sales

# COMMAND ----------

df_sale.display()

# COMMAND ----------

df_sale=df_sale.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sale=df_sale.withColumn('OrderNumer',regexp_replace('OrderNumber','S','T'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sale Analysis

# COMMAND ----------

df_sale.groupBy('OrderDate').agg(count('OrderNumber').alias("TotalOrders")).display()

# COMMAND ----------

df_pro_cat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC