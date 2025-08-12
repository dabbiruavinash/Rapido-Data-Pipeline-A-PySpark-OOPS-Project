from pyspark.sql import DataFrame
from utils.logger import Logger
from services.storage_service import StorageService

class GoldPipeline:
    """Pipeline for curating data in Gold layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.storage = StorageService(spark, config)
        self.logger = Logger.get_logger()
        
    def process_to_gold(self, silver_tables: dict) -> dict:
        """Read from silver and process to gold datasets"""
        self.logger.info("Processing to gold layer")
        
        gold_datasets = {}
        
        # Process ride analytics
        if "rides" in silver_tables:
            from jobs.curation.ride_analytics import RideAnalytics
            ride_analytics = RideAnalytics(self.spark)
            silver_rides = self.storage.read_delta("silver", "rides")
            gold_datasets["daily_ride_metrics"] = ride_analytics.create_daily_ride_metrics(silver_rides)
            gold_datasets["ride_geo_heatmap"] = ride_analytics.create_geo_heatmap(silver_rides)
        
        # Process driver analytics
        if "drivers" in silver_tables and "rides" in silver_tables:
            from jobs.curation.driver_analytics import DriverAnalytics
            driver_analytics = DriverAnalytics(self.spark)
            silver_drivers = self.storage.read_delta("silver", "drivers")
            silver_rides = self.storage.read_delta("silver", "rides")
            gold_datasets["driver_performance"] = driver_analytics.create_driver_performance(silver_drivers, silver_rides)
            gold_datasets["driver_geo_distribution"] = driver_analytics.create_driver_geo_distribution(silver_drivers)
        
        # Process customer analytics
        if "customers" in silver_tables and "rides" in silver_tables:
            from jobs.curation.customer_analytics import CustomerAnalytics
            customer_analytics = CustomerAnalytics(self.spark)
            silver_customers = self.storage.read_delta("silver", "customers")
            silver_rides = self.storage.read_delta("silver", "rides")
            gold_datasets["customer_segments"] = customer_analytics.create_customer_segments(silver_customers)
            gold_datasets["customer_retention"] = customer_analytics.create_retention_metrics(silver_customers, silver_rides)
            
        return gold_datasets
    
    def write_to_gold(self, df: DataFrame, table_name: str):
        """Write curated data to gold layer"""
        output_path = f"{self.config['gold_path']}/{table_name}"
        
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(output_path)
        
        # Optimize and Z-order by common query patterns
        self.storage.optimize_table("gold", table_name)
        self.spark.sql(f"""
            OPTIMIZE delta.`{output_path}` 
            ZORDER BY ({self._get_zorder_columns(table_name)})
        """)
    
    def _get_zorder_columns(self, table_name: str) -> str:
        """Get appropriate ZORDER BY columns for each table"""
        zorders = {
            "daily_ride_metrics": "ride_date, driver_id",
            "driver_performance": "driver_id",
            "customer_segments": "customer_segment"
        }
        return zorders.get(table_name, "1")  # Default to no zordering if not specified