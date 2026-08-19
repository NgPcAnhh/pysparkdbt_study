# Databricks notebook source
array = [
    {
        "name": "customers", 
        "schema": (
            "StructType([\n"
            "    StructField('customer_id', IntegerType(), True),\n"
            "    StructField('first_name', StringType(), True),\n"
            "    StructField('last_name', StringType(), True),\n"
            "    StructField('email', StringType(), True),\n"
            "    StructField('phone_number', StringType(), True),\n"
            "    StructField('city', StringType(), True),\n"
            "    StructField('signup_date', DateType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    },
    {
        "name": "locations", 
        "schema": (
            "StructType([\n"
            "    StructField('location_id', IntegerType(), True),\n"
            "    StructField('city', StringType(), True),\n"
            "    StructField('state', StringType(), True),\n"
            "    StructField('country', StringType(), True),\n"
            "    StructField('latitude', DoubleType(), True),\n"
            "    StructField('longitude', DoubleType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    },
    {
        "name": "trips", 
        "schema": (
            "StructType([\n"
            "    StructField('trip_id', IntegerType(), True),\n"
            "    StructField('driver_id', IntegerType(), True),\n"
            "    StructField('customer_id', IntegerType(), True),\n"
            "    StructField('vehicle_id', IntegerType(), True),\n"
            "    StructField('trip_start_time', TimestampType(), True),\n"
            "    StructField('trip_end_time', TimestampType(), True),\n"
            "    StructField('start_location', StringType(), True),\n"
            "    StructField('end_location', StringType(), True),\n"
            "    StructField('distance_km', DoubleType(), True),\n"
            "    StructField('fare_amount', DoubleType(), True),\n"
            "    StructField('payment_method', StringType(), True),\n"
            "    StructField('trip_status', StringType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    },
    {
        "name": "payments", 
        "schema": (
            "StructType([\n"
            "    StructField('payment_id', IntegerType(), True),\n"
            "    StructField('trip_id', IntegerType(), True),\n"
            "    StructField('customer_id', IntegerType(), True),\n"
            "    StructField('payment_method', StringType(), True),\n"
            "    StructField('payment_status', StringType(), True),\n"
            "    StructField('amount', DoubleType(), True),\n"
            "    StructField('transaction_time', TimestampType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    },
    {
        "name": "drivers", 
        "schema": (
            "StructType([\n"
            "    StructField('driver_id', IntegerType(), True),\n"
            "    StructField('first_name', StringType(), True),\n"
            "    StructField('last_name', StringType(), True),\n"
            "    StructField('phone_number', StringType(), True),\n"
            "    StructField('vehicle_id', IntegerType(), True),\n"
            "    StructField('driver_rating', DoubleType(), True),\n"
            "    StructField('city', StringType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    },
    {
        "name": "vehicles", 
        "schema": (
            "StructType([\n"
            "    StructField('vehicle_id', IntegerType(), True),\n"
            "    StructField('license_plate', StringType(), True),\n"
            "    StructField('model', StringType(), True),\n"
            "    StructField('make', StringType(), True),\n"
            "    StructField('year', IntegerType(), True),\n"
            "    StructField('vehicle_type', StringType(), True),\n"
            "    StructField('last_updated_timestamp', TimestampType(), True)\n"
            "])"
        )
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK STREAMING
# MAGIC

# COMMAND ----------

entities = ['customers', 'locations', 'trips', 'payments', 'drivers', 'vehicles']

# COMMAND ----------

# DBTITLE 1,i
for entity in entities:
    # 1. Đọc batch để infer schema động cho từng entity
    df_batch = (
        spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(f"/Volumes/pysparkdbt/source/source/{entity}/{entity}.csv")
    )

    # 2. Lấy schema chuẩn của entity hiện tại
    schema_entity = df_batch.schema

    # 3. Đọc dữ liệu stream bằng schema vừa lấy
    df = (
        spark.readStream
        .format("csv")
        .option("header", True)
        .schema(schema_entity)
        .load(f"/Volumes/pysparkdbt/source/source/{entity}/")
    )

    # 4. Ghi stream vào Delta Table
    (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"/Volumes/pysparkdbt/bronze/checkpoint/{entity}/")
        .trigger(once=True)
        .toTable(f"pysparkdbt.bronze.{entity}")
    )

# COMMAND ----------

