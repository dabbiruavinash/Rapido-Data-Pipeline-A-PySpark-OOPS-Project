from pyspark.sql import DataFrame, functions as F
from models.driver import DriverSchema
from utils.logger import Logger

class DriverProcessor:
    """Processor for transforming driver data in Silver layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def process(self, bronze_df: DataFrame) -> DataFrame:
        """Transform raw driver data into canonical form"""
        self.logger.info("Processing driver data for silver layer")
        
        # Filter for driver events
        driver_events = bronze_df.filter(
            F.col("event_type").isin(["driver_registered", "driver_activated", "driver_location_update"])
        
        # Pivot to get driver attributes
        silver_drivers = driver_events.groupBy("driver_id") \
            .pivot("event_type", ["driver_registered", "driver_activated"]) \
            .agg(
                F.first("event_timestamp").alias("timestamp"),
                F.first("latitude").alias("lat"),
                F.first("longitude").alias("long")
            )
            
        # Add current location from latest location update
        window = Window.partitionBy("driver_id").orderBy(F.desc("event_timestamp"))
        latest_location = driver_events.filter(F.col("event_type") == "driver_location_update") \
            .withColumn("rank", F.rank().over(window)) \
            .filter(F.col("rank") == 1) \
            .select("driver_id", "latitude", "longitude")
            
        silver_drivers = silver_drivers.join(latest_location, "driver_id", "left")
        
        return DriverSchema.enforce_schema(silver_drivers)