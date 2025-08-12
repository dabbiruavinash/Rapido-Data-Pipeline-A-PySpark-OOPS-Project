from pyspark.sql import DataFrame, functions as F
from utils.logger import Logger

class DriverAnalytics:
    """Creates curated datasets for driver analytics in Gold layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def create_driver_performance(self, silver_drivers: DataFrame, silver_rides: DataFrame) -> DataFrame:
        """Create driver performance metrics dataset"""
        self.logger.info("Creating driver performance metrics")
        
        # Calculate ride metrics per driver
        ride_metrics = silver_rides.groupBy("driver_id") \
            .agg(
                F.count("*").alias("total_rides"),
                F.avg("duration_seconds").alias("avg_ride_duration"),
                F.sum(F.when(F.col("ride_cancelled_timestamp").isNotNull(), 1).otherwise(0)).alias("cancelled_rides")
            )
            
        # Join with driver attributes
        gold_driver_performance = silver_drivers.join(ride_metrics, "driver_id", "left") \
            .withColumn("cancellation_rate", F.col("cancelled_rides") / F.col("total_rides")) \
            .withColumn("active_status", 
                F.when(F.datediff(F.current_date(), F.col("driver_activated_timestamp")) < 30, "new")
                .otherwise("active"))
                
        return gold_driver_performance
    
    def create_driver_geo_distribution(self, silver_drivers: DataFrame) -> DataFrame:
        """Create driver geographic distribution dataset"""
        self.logger.info("Creating driver geo distribution")
        
        # Round coordinates for aggregation
        gold_driver_geo = silver_drivers.withColumn("lat_rounded", F.round(F.col("latitude"), 2)) \
            .withColumn("long_rounded", F.round(F.col("longitude"), 2)) \
            .groupBy("lat_rounded", "long_rounded") \
            .agg(F.count("*").alias("driver_count"))
            
        return gold_driver_geo