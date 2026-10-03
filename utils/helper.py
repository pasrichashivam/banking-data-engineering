import uuid
from datetime import datetime
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DecimalType, DateType, TimestampType, BooleanType,
)

batch_control_schema = StructType([
    StructField("ingestion_run_id", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("target_layer", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("record_count", LongType(), True),
    StructField("error_message", StringType(), True),
])

class BatchControlHelper:
    def __init__(self, spark, batch_control_table, watermark_table):
        self.spark = spark
        self.batch_control_table = batch_control_table
        self.watermark_table = watermark_table

    
    def has_succeeded(self, batch_id: str, source_table: str) -> bool:
        if not self.spark.catalog.tableExists(self.batch_control_table):
            return False
        existing = self.spark.table(self.batch_control_table).filter(
            (f.col("batch_id") == batch_id)
            & (f.col("source_table") == source_table)
            & (f.col("status") == "SUCCESS")
        )
        return existing.limit(1).count() > 0


    def start_batch(self, batch_id: str, source_table: str, target_table: str, target_layer: str, load_type: str) -> str:
        run_id = str(uuid.uuid4())
        row = self.spark.createDataFrame(
            [(run_id, batch_id, source_table, target_table, target_layer, load_type, "RUNNING", datetime.utcnow(), None, None, None)],
            schema=batch_control_schema,
        )
        row.write.format("delta").mode("append").saveAsTable(self.batch_control_table)
        return run_id

    def complete_batch(self, run_id: str, record_count: int) -> None:
        DeltaTable.forName(self.spark, self.batch_control_table).update(
            condition=f"ingestion_run_id = '{run_id}'",
            set={
                "status": f.lit("SUCCESS"),
                "end_time": f.current_timestamp(),
                "record_count": f.lit(record_count),
            },
        )


    def fail_batch(self, run_id: str, error_message: str) -> None:
        safe_message = error_message.replace("'", "''")[:500]
        DeltaTable.forName(self.spark, self.batch_control_table).update(
            condition=f"ingestion_run_id = '{run_id}'",
            set={
                "status": f.lit("FAILED"),
                "end_time": f.current_timestamp(),
                "error_message": f.lit(safe_message),
            },
        )

    def get_watermark(self, table_name: str, process_name:str):
        rows = (
            self.spark.table(self.watermark_table).filter((f.col("table_name") == table_name) & (f.col("process_name") == process_name))
            .select("last_source_timestamp").limit(1).collect()
        )
        return rows[0]["last_source_timestamp"] if rows else None
    

    def update_watermark(self, process_name:str, table_name: str, source_max_timestamp, run_id: str) -> None:
        watermark_row = self.spark.createDataFrame(
            [(process_name, table_name, source_max_timestamp, run_id)],
            schema="process_name STRING, table_name STRING, "
            "last_source_timestamp TIMESTAMP, last_ingestion_run_id STRING",
        ).withColumn("last_updated_at", f.current_timestamp())
        
        watermark_delta = DeltaTable.forName(self.spark, self.watermark_table)
        
        watermark_delta.alias("target").merge(watermark_row.alias("source"),
            condition=f"target.process_name = source.process_name AND target.table_name = source.table_name",
        ).whenMatchedUpdate(set={
            "last_source_timestamp": (
                "greatest(target.last_source_timestamp, source.last_source_timestamp)"
            ),
            "last_updated_at": "source.last_updated_at",
            "last_ingestion_run_id": "source.last_ingestion_run_id",
        }).whenNotMatchedInsertAll().execute()
    