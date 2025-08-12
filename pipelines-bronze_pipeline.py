from pyspark.sql import DataFrame
from models.event import EventSchema
from utils.schema_utils import SchemaUtils
from utils.logger import Logger

class BronzePipeline:
    """Pipeline for ingesting raw data into Bronze layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.logger = Logger.get_logger()
        
    def process_stream(self, source_df: DataFrame) -> DataFrame:
        """Process streaming data into bronze layer"""
        self.logger.info("Starting bronze layer processing")
        
        # Parse the Kafka value field which contains the JSON data
        json_schema = EventSchema.get_schema()
        bronze_df = source_df.selectExpr("CAST(value AS STRING) as json") \
            .select(SchemaUtils.from_json("json", json_schema).alias("data")) \
            .select("data.*")
            
        # Add metadata columns
        bronze_df = bronze_df.withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("year", F.year("event_timestamp")) \
            .withColumn("month", F.month("event_timestamp")) \
            .withColumn("day", F.dayofmonth("event_timestamp"))
            
        return bronze_df
    
    def write_to_bronze(self, df: DataFrame, table_name: str):
        """Write processed data to bronze layer storage"""
        output_path = f"{self.config['bronze_path']}/{table_name}"
        
        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.config['checkpoint_path']}/bronze/{table_name}") \
            .partitionBy("year", "month", "day") \
            .start(output_path)