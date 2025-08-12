from pyspark.sql import DataFrame
from utils.logger import Logger
from services.storage_service import StorageService

class SilverPipeline:
    """Pipeline for processing data in Silver layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.storage = StorageService(spark, config)
        self.logger = Logger.get_logger()
        
    def process_to_silver(self, bronze_table: str) -> DataFrame:
        """Read from bronze and process to silver"""
        self.logger.info(f"Processing {bronze_table} to silver layer")
        
        bronze_df = self.storage.read_delta("bronze", bronze_table)
        
        if bronze_table == "rides":
            from jobs.processing.ride_processor import RideProcessor
            processor = RideProcessor(self.spark)
            return processor.process(bronze_df)
        elif bronze_table == "drivers":
            from jobs.processing.driver_processor import DriverProcessor
            processor = DriverProcessor(self.spark)
            return processor.process(bronze_df)
        elif bronze_table == "customers":
            from jobs.processing.customer_processor import CustomerProcessor
            processor = CustomerProcessor(self.spark)
            return processor.process(bronze_df)
        
    def write_to_silver(self, df: DataFrame, table_name: str):
        """Write processed data to silver layer"""
        output_path = f"{self.config['silver_path']}/{table_name}"
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        # Optimize the table after write
        self.storage.optimize_table("silver", table_name)