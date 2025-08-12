from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from utils.logger import Logger

class RideAnalytics:
    """Creates curated datasets for ride analytics in Gold layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def create_daily_ride_metrics(self, silver_rides: DataFrame) -> DataFrame:
        """Create daily ride metrics dataset"""
        self.logger.info("Creating daily ride metrics")
        
        gold_metrics = silver_rides.groupBy(
            F.to_date("ride_started_timestamp").alias("ride_date"),
            "driver_id"
        ).agg(
            F.count("*").alias("total_rides"),
            F.sum("duration_seconds").alias("total_duration"),
            F.avg("duration_seconds").alias("avg_duration"),
            F.sum(F.when(F.col("ride_cancelled_timestamp").isNotNull(), 1).otherwise(0)).alias("cancelled_rides")
        ).withColumn(
            "cancellation_rate",
            F.col("cancelled_rides") / F.col("total_rides")
        )
        
        return gold_metrics
    
    def create_geo_heatmap(self, silver_rides: DataFrame) -> DataFrame:
        """Create geo heatmap dataset for visualization"""
        self.logger.info("Creating geo heatmap data")
        
        # Round coordinates to 2 decimal places for heatmap aggregation
        gold_heatmap = silver_rides.withColumn(
            "start_lat_rounded", F.round(F.col("ride_started_lat"), 2)
        ).withColumn(
            "start_long_rounded", F.round(F.col("ride_started_long"), 2)
        ).groupBy(
            "start_lat_rounded", "start_long_rounded"
        ).agg(
            F.count("*").alias("ride_count"),
            F.avg("duration_seconds").alias("avg_duration")
        )
        
        return gold_heatmap