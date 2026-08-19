# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_cust = spark.read.table("pysparkdbt.bronze.customers")

# COMMAND ----------

df_cust = df_cust.withColumn("domain", split(col("email"), "@")[1])
df_cust = df_cust.withColumn("phone_number", regexp_replace(col("phone_number"), r"[^0-9]", ""))
df_cust = df_cust.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name")))
df_cust = df_cust.drop("first_name","last_name")

display(df_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC import gán notebook vào path của DATABRICK và thử import class Transformation

# COMMAND ----------

from utils.custom_utils import transformations
import os 
import sys

current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CUSTOMER
# MAGIC

# COMMAND ----------

cust_obj = transformations()

cust_df_transf = cust_obj.dedup(df_cust, ["customer_id"], 'last_updated_timestamp')
display(cust_df_transf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.bronze.customers

# COMMAND ----------

# Gọi hàm upsert
cust_obj.upsert(
    df=cust_df_transf,
    target_table="customers",
    target_table_schema="pysparkdbt.silver",
    target_table_pk="customer_id",
    target_table_cdc="last_updated_timestamp"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### DRIVERS
# MAGIC
# MAGIC

# COMMAND ----------

df_driver = spark.read.table("pysparkdbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn("phone_number", regexp_replace(col("phone_number"), r"[^0-9]", ""))
df_driver = df_driver.withColumn("full_name",concat_ws(" ",col("first_name"),col("last_name")))
df_driver = df_driver.drop("first_name","last_name")

# COMMAND ----------

driver_obj = transformations()

df_driver = driver_obj.dedup(df_driver, ["driver_id"], 'last_updated_timestamp')
df_driver = driver_obj.upsert(
    df=df_driver,
    target_table="drivers",
    target_table_schema="pysparkdbt.silver",
    target_table_pk="driver_id",
    target_table_cdc="last_updated_timestamp"
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.bronze.drivers
# MAGIC     union all 
# MAGIC select count(*) from pysparkdbt.silver.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOCATIONS

# COMMAND ----------

df_loc = spark.read.table("pysparkdbt.bronze.locations")
display(df_loc)

# COMMAND ----------

location_obj = transformations()

df_loc = location_obj.dedup(df_loc, ["location_id"], 'last_updated_timestamp')
df_loc = location_obj.upsert(
    df=df_loc,
    target_table="locations",
    target_table_schema="pysparkdbt.silver",
    target_table_pk="location_id",
    target_table_cdc="last_updated_timestamp"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM pysparkdbt.bronze.locations
# MAGIC UNION ALL 
# MAGIC SELECT count(*) FROM pysparkdbt.silver.locations

# COMMAND ----------

# MAGIC %md
# MAGIC ### PAYMENTS

# COMMAND ----------

df_payments = spark.read.table("pysparkdbt.bronze.payments")


# COMMAND ----------

# transformation
df_payments = df_payments.withColumn("online_payment_status",
                when(((col("payment_method") == "Card") & (col("payment_status") == "Success")),"online-success")
                .when(((col("payment_method") == "Card") & (col("payment_status") == "Failed")),"online-failed")
                .when(((col("payment_method") == "Card") & (col("payment_status") == "Peding")),"online-pending")
                .otherwise("offline") 
)
display(df_payments)


# COMMAND ----------

payment_obj = transformations()

df_payments = payment_obj.dedup(df_payments, ["payment_id"], 'last_updated_timestamp')
df_payments = payment_obj.upsert(
    df=df_payments,
    target_table="payments",
    target_table_schema="pysparkdbt.silver",
    target_table_pk="payment_id",
    target_table_cdc="last_updated_timestamp"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.bronze.payments
# MAGIC union all
# MAGIC select count(*) from pysparkdbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ### VEHICLES
# MAGIC

# COMMAND ----------

df_vehicle = spark.read.table('pysparkdbt.bronze.vehicles')
display(df_vehicle)

# COMMAND ----------

df_vehicle = df_vehicle.withColumn("make",upper(col("make")))
display(df_vehicle)

# COMMAND ----------

vehicle_obj = transformations()

df_vehicle = vehicle_obj.dedup(df_vehicle, ["vehicle_id"], 'last_updated_timestamp')
df_vehicle = vehicle_obj.upsert(
    df=df_vehicle,
    target_table="vehicles",
    target_table_schema="pysparkdbt.silver",
    target_table_pk="vehicle_id",
    target_table_cdc="last_updated_timestamp"
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from pysparkdbt.bronze.vehicles
# MAGIC union all 
# MAGIC select count(*) from pysparkdbt.silver.vehicles

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC