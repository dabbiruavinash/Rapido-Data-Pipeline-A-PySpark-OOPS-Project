from pyspark.sql import DataFrame
from utils.logger import Logger
from services.storage_service import StorageService

class BatchIngestor:
    """Handles batch ingestion of historical data into Bronze layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.storage = StorageService(spark, config)
        self.logger = Logger.get_logger()
        
    def ingest_csv(self, file_path: str, table_name: str, schema):
        """Ingest CSV files into bronze layer"""
        self.logger.info(f"Ingesting CSV from {file_path} to {table_name}")
        
        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(file_path)
            
        self._add_metadata_columns(df)
        self.storage.write_delta(df, "bronze", table_name)
        
    def ingest_json(self, file_path: str, table_name: str, schema):
        """Ingest JSON files into bronze layer"""
        self.logger.info(f"Ingesting JSON from {file_path} to {table_name}")
        
        df = self.spark.read \
            .schema(schema) \
            .json(file_path)
            
        self._add_metadata_columns(df)
        self.storage.write_delta(df, "bronze", table_name)
        
    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """Add common metadata columns for batch ingestion"""
        return df.withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("year", F.year(F.current_date())) \
            .withColumn("month", F.month(F.current_date())) \
            .withColumn("day", F.dayofmonth(F.current_date()))