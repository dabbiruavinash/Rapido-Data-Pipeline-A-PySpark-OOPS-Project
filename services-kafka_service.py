from pyspark.sql import SparkSession
from typing import Dict, Any

class KafkaService:
    """Service for interacting with Kafka event bus"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        
    def read_stream(self, topic: str):
        """Read from Kafka as a streaming DataFrame"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config["bootstrap_servers"]) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
    
    def write_stream(self, df, topic: str, checkpoint_location: str):
        """Write to Kafka as a streaming sink"""
        return df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config["bootstrap_servers"]) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_location) \
            .start()