from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

class EventSchema:
    """Defines the schema for event data"""
    
    @staticmethod
    def get_schema():
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("user_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("ride_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("event_data", StringType(), True),
            StructField("source", StringType(), False)
        ])