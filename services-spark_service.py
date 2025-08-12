from pyspark.sql import SparkSession
from typing import Dict, Any

class SparkService:
    """Service for managing Spark session and configurations"""
    
    _instance = None
    
    def __new__(cls, config: Dict[str, Any]):
        if cls._instance is None:
            cls._instance = super(SparkService, cls).__new__(cls)
            cls._instance.initialize_spark(config)
        return cls._instance
    
    def initialize_spark(self, config: Dict[str, Any]):
        """Initialize Spark session with given configuration"""
        self.spark = SparkSession.builder \
            .appName(config.get("app_name", "rapido-data-pipeline")) \
            .config("spark.sql.shuffle.partitions", config.get("shuffle_partitions", "200")) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0") \
            .getOrCreate()
            
        # Additional Delta Lake optimizations
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        
    def get_spark(self) -> SparkSession:
        """Get the Spark session instance"""
        return self.spark