from pyspark.sql import DataFrame
from typing import Dict, Any

class StorageService:
    """Service for interacting with different storage layers"""
    
    def __init__(self, spark, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        
    def read_delta(self, layer: str, table_name: str) -> DataFrame:
        """Read data from Delta table"""
        path = f"{self.config[f'{layer}_path']}/{table_name}"
        return self.spark.read.format("delta").load(path)
        
    def write_delta(self, df: DataFrame, layer: str, table_name: str, mode: str = "append"):
        """Write data to Delta table"""
        path = f"{self.config[f'{layer}_path']}/{table_name}"
        df.write.format("delta").mode(mode).save(path)
        
    def optimize_table(self, layer: str, table_name: str):
        """Run OPTIMIZE on Delta table"""
        path = f"{self.config[f'{layer}_path']}/{table_name}"
        self.spark.sql(f"OPTIMIZE delta.`{path}`")