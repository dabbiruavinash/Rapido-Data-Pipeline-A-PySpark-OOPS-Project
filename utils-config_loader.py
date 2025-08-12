import json
from typing import Dict, Any

class ConfigLoader:
    """Utility for loading configuration files"""
    
    @staticmethod
    def load_config(file_path: str = "configs/spark_config.json") -> Dict[str, Any]:
        """Load configuration from JSON file"""
        with open(file_path, "r") as f:
            config = json.load(f)
        return config
        
    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Get Kafka-specific configuration"""
        return ConfigLoader.load_config("configs/kafka_config.json")
        
    @staticmethod
    def get_storage_config() -> Dict[str, Any]:
        """Get storage-specific configuration"""
        return ConfigLoader.load_config("configs/aws_config.json")