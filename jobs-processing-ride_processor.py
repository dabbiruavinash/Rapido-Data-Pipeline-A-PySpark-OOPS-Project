from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from models.ride import RideSchema
from utils.logger import Logger

class RideProcessor:
    """Processor for transforming ride data in Silver layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def process(self, bronze_df: DataFrame) -> DataFrame:
        """Transform raw ride data into canonical form"""
        self.logger.info("Processing ride data for silver layer")
        
        # Filter for ride events and apply schema
        ride_events = bronze_df.filter(
            (F.col("event_type").isin(["ride_started", "ride_completed", "ride_cancelled"]))
        )
        
        # Pivot to get ride attributes
        silver_rides = ride_events.groupBy("ride_id") \
            .pivot("event_type", ["ride_started", "ride_completed", "ride_cancelled"]) \
            .agg(
                F.first("event_timestamp").alias("timestamp"),
                F.first("latitude").alias("lat"),
                F.first("longitude").alias("long")
            )
            
        # Calculate ride duration for completed rides
        silver_rides = silver_rides.withColumn(
            "duration_seconds",
            F.when(
                F.col("ride_completed_timestamp").isNotNull(),
                F.unix_timestamp("ride_completed_timestamp") - F.unix_timestamp("ride_started_timestamp")
            ).otherwise(None)
        )
        
        # Add derived columns
        silver_rides = silver_rides.withColumn("processing_date", F.current_date())
        
        return RideSchema.enforce_schema(silver_rides)