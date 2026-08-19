from typing import List
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql import SparkSession 

class transformations:
    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str) -> DataFrame:
        windowSpec = Window.partitionBy(*dedup_cols).orderBy(col(cdc).asc())
        
        df = df.withColumn("dedupCounts", row_number().over(windowSpec))
        df = df.filter(col("dedupCounts") == 1).drop("dedupCounts")
        
        return df

    def process_timestamp(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(
        self,
        df: DataFrame,
        target_table: str,
        target_table_schema: str,
        target_table_pk: str,
        target_table_cdc: str,
    ) -> None:
        # 2. Lấy phiên làm việc Spark đang active trong Databricks
        spark = SparkSession.getActiveSession()

        full_table_name = f"{target_table_schema}.{target_table}"

        # 3. Code bên dưới giữ nguyên
        if spark.catalog.tableExists(full_table_name):
            deltaTable = DeltaTable.forName(spark, full_table_name)
            merge_condition = f"target.{target_table_pk} = source.{target_table_pk}"
            update_condition = (
                f"source.{target_table_cdc} >= target.{target_table_cdc}"
            )

            (
                deltaTable.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll(condition=update_condition)
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").saveAsTable(
                full_table_name
            )