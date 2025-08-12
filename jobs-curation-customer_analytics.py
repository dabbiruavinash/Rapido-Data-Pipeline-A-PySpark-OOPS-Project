from pyspark.sql import DataFrame, functions as F
from utils.logger import Logger

class CustomerAnalytics:
    """Creates curated datasets for customer analytics in Gold layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def create_customer_segments(self, silver_customers: DataFrame) -> DataFrame:
        """Create customer segmentation dataset"""
        self.logger.info("Creating customer segments")
        
        gold_segments = silver_customers.withColumn("customer_segment",
            F.when(F.col("total_rides_requested") > 20, "frequent")
            .when(F.col("total_rides_requested") > 5, "regular")
            .otherwise("occasional"))
            
        return gold_segments
    
    def create_retention_metrics(self, silver_customers: DataFrame, silver_rides: DataFrame) -> DataFrame:
        """Create customer retention metrics"""
        self.logger.info("Creating customer retention metrics")
        
        # Calculate last ride date per customer
        last_ride_dates = silver_rides.groupBy("user_id") \
            .agg(F.max("ride_started_timestamp").alias("last_ride_date"))
            
        gold_retention = silver_customers.join(last_ride_dates, "user_id", "left") \
            .withColumn("days_since_last_ride", 
                F.datediff(F.current_date(), F.col("last_ride_date"))) \
            .withColumn("retention_status",
                F.when(F.col("days_since_last_ride") < 7, "active")
                .when(F.col("days_since_last_ride") < 30, "lapsing")
                .otherwise("churned"))
                
        return gold_retention