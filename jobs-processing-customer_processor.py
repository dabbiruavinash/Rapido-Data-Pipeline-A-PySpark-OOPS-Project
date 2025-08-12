from pyspark.sql import DataFrame, functions as F
from models.customer import CustomerSchema
from utils.logger import Logger

class CustomerProcessor:
    """Processor for transforming customer data in Silver layer"""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = Logger.get_logger()
        
    def process(self, bronze_df: DataFrame) -> DataFrame:
        """Transform raw customer data into canonical form"""
        self.logger.info("Processing customer data for silver layer")
        
        # Filter for customer events
        customer_events = bronze_df.filter(
            F.col("event_type").isin(["customer_registered", "customer_ride_request"])
        
        # Create customer profile with first registration details
        silver_customers = customer_events.filter(F.col("event_type") == "customer_registered") \
            .groupBy("user_id") \
            .agg(
                F.first("event_timestamp").alias("registration_date"),
                F.first("latitude").alias("home_location_lat"),
                F.first("longitude").alias("home_location_long")
            )
            
        # Add ride count metrics
        ride_counts = customer_events.filter(F.col("event_type") == "customer_ride_request") \
            .groupBy("user_id") \
            .agg(F.count("*").alias("total_rides_requested"))
            
        silver_customers = silver_customers.join(ride_counts, "user_id", "left")
        
        return CustomerSchema.enforce_schema(silver_customers)