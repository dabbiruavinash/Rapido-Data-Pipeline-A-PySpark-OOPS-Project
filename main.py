from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from jobs.ingestion.event_ingestor import EventIngestor
from jobs.ingestion.batch_ingestor import BatchIngestor
from jobs.processing.ride_processor import RideProcessor
from jobs.processing.driver_processor import DriverProcessor
from jobs.processing.customer_processor import CustomerProcessor
from jobs.curation.ride_analytics import RideAnalytics
from jobs.curation.driver_analytics import DriverAnalytics
from jobs.curation.customer_analytics import CustomerAnalytics

from services.spark_service import SparkService
from services.kafka_service import KafkaService
from services.storage_service import StorageService
from services.monitoring_service import MonitoringService

from pipelines.bronze_pipeline import BronzePipeline
from pipelines.silver_pipeline import SilverPipeline
from pipelines.gold_pipeline import GoldPipeline

from models.event import EventSchema
from models.ride import RideSchema
from models.driver import DriverSchema
from models.customer import CustomerSchema

from utils.config_loader import ConfigLoader
from utils.logger import Logger
from utils.schema_utils import SchemaUtils
from utils.date_utils import DateUtils

import argparse
import sys
from typing import Dict, Any, List

class RapidoDataPipeline:
    """Main application class for the Rapido data pipeline"""
    
    def __init__(self, environment: str = "prod"):
        # Initialize configuration based on environment
        self.config = ConfigLoader.load_config(f"configs/{environment}_config.json")
        self.environment = environment
        self.logger = Logger.get_logger()
        
        # Initialize services
        self.spark_service = SparkService(self.config["spark"])
        self.spark = self.spark_service.get_spark()
        self.kafka_service = KafkaService(self.spark, self.config["kafka"])
        self.storage_service = StorageService(self.spark, self.config["storage"])
        self.monitoring = MonitoringService(self.spark, self.config["monitoring"])
        
        # Initialize pipelines
        self.bronze_pipeline = BronzePipeline(self.spark, self.config)
        self.silver_pipeline = SilverPipeline(self.spark, self.config)
        self.gold_pipeline = GoldPipeline(self.spark, self.config)
        
        # Track active streaming queries
        self.active_queries: List[StreamingQuery] = []
    
    def run_streaming_pipeline(self):
        """Run the complete streaming pipeline"""
        try:
            self.logger.info("Starting Rapido Streaming Data Pipeline")
            
            # 1. Bronze Layer - Stream Ingestion
            self.logger.info("Starting Bronze Layer Processing")
            raw_stream = self.kafka_service.read_stream(self.config["kafka"]["input_topic"])
            bronze_stream = self.bronze_pipeline.process_stream(raw_stream)
            
            # Write bronze data to Delta with checkpointing
            bronze_query = self.bronze_pipeline.write_to_bronze(
                bronze_stream, 
                "events",
                checkpoint_location=f"{self.config['storage']['checkpoint_path']}/bronze/events"
            )
            self.active_queries.append(bronze_query)
            
            # 2. Silver Layer - Processing
            self.logger.info("Starting Silver Layer Processing")
            
            # Process rides
            silver_rides = RideProcessor(self.spark).process(bronze_stream)
            rides_query = self.silver_pipeline.write_to_silver(
                silver_rides,
                "rides",
                checkpoint_location=f"{self.config['storage']['checkpoint_path']}/silver/rides"
            )
            self.active_queries.append(rides_query)
            
            # Process drivers
            silver_drivers = DriverProcessor(self.spark).process(bronze_stream)
            drivers_query = self.silver_pipeline.write_to_silver(
                silver_drivers,
                "drivers",
                checkpoint_location=f"{self.config['storage']['checkpoint_path']}/silver/drivers"
            )
            self.active_queries.append(drivers_query)
            
            # Process customers
            silver_customers = CustomerProcessor(self.spark).process(bronze_stream)
            customers_query = self.silver_pipeline.write_to_silver(
                silver_customers,
                "customers",
                checkpoint_location=f"{self.config['storage']['checkpoint_path']}/silver/customers"
            )
            self.active_queries.append(customers_query)
            
            # 3. Gold Layer - Analytics
            self.logger.info("Starting Gold Layer Processing")
            
            # Read from silver (batch) for gold processing
            silver_rides_batch = self.storage_service.read_delta("silver", "rides")
            silver_drivers_batch = self.storage_service.read_delta("silver", "drivers")
            silver_customers_batch = self.storage_service.read_delta("silver", "customers")
            
            # Create gold datasets
            gold_datasets = {
                "ride_metrics": RideAnalytics(self.spark).create_daily_ride_metrics(silver_rides_batch),
                "driver_performance": DriverAnalytics(self.spark).create_driver_performance(
                    silver_drivers_batch, silver_rides_batch
                ),
                "customer_segments": CustomerAnalytics(self.spark).create_customer_segments(silver_customers_batch)
            }
            
            # Write gold datasets
            for name, dataset in gold_datasets.items():
                self.gold_pipeline.write_to_gold(dataset, name)
                self.monitoring.log_metrics({
                    f"{name}_row_count": dataset.count(),
                    f"{name}_processed_at": datetime.now().isoformat()
                })
            
            self.logger.info("Pipeline started successfully. Waiting for termination...")
            self._await_termination()
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            self.monitoring.send_alert(f"Pipeline failed: {str(e)}")
            raise
    
    def run_batch_pipeline(self, historical_data_path: str):
        """Run batch processing for historical data"""
        try:
            self.logger.info("Starting Rapido Batch Data Pipeline")
            
            # 1. Bronze Layer - Batch Ingestion
            self.logger.info("Ingesting historical data to Bronze Layer")
            batch_ingestor = BatchIngestor(self.spark, self.config)
            
            # Ingest events
            batch_ingestor.ingest_json(
                f"{historical_data_path}/events/*.json",
                "historical_events",
                EventSchema.get_schema()
            )
            
            # 2. Silver Layer - Batch Processing
            self.logger.info("Processing historical data to Silver Layer")
            bronze_events = self.storage_service.read_delta("bronze", "historical_events")
            
            # Process historical rides
            historical_rides = RideProcessor(self.spark).process(bronze_events)
            self.silver_pipeline.write_to_silver(historical_rides, "historical_rides")
            
            # Process historical drivers
            historical_drivers = DriverProcessor(self.spark).process(bronze_events)
            self.silver_pipeline.write_to_silver(historical_drivers, "historical_drivers")
            
            # Process historical customers
            historical_customers = CustomerProcessor(self.spark).process(bronze_events)
            self.silver_pipeline.write_to_silver(historical_customers, "historical_customers")
            
            # 3. Gold Layer - Historical Analytics
            self.logger.info("Creating historical analytics in Gold Layer")
            gold_datasets = {
                "historical_ride_metrics": RideAnalytics(self.spark).create_daily_ride_metrics(historical_rides),
                "historical_driver_performance": DriverAnalytics(self.spark).create_driver_performance(
                    historical_drivers, historical_rides
                )
            }
            
            for name, dataset in gold_datasets.items():
                self.gold_pipeline.write_to_gold(dataset, name)
            
            self.logger.info("Batch pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Batch pipeline failed: {str(e)}", exc_info=True)
            self.monitoring.send_alert(f"Batch pipeline failed: {str(e)}")
            raise
    
    def _await_termination(self):
        """Wait for streaming queries to terminate"""
        while True:
            for query in list(self.active_queries):
                if query.isActive:
                    try:
                        query.awaitTermination(5)
                    except Exception as e:
                        self.logger.warning(f"Query {query.id} encountered error: {str(e)}")
                        self.active_queries.remove(query)
                else:
                    self.active_queries.remove(query)
            
            if not self.active_queries:
                self.logger.info("All streaming queries have terminated")
                break
    
    def shutdown(self):
        """Gracefully shutdown the pipeline"""
        self.logger.info("Shutting down pipeline")
        for query in self.active_queries:
            query.stop()
        self.spark.stop()

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Rapido Data Pipeline")
    parser.add_argument("--env", default="prod", choices=["dev", "staging", "prod"],
                      help="Execution environment")
    parser.add_argument("--mode", default="streaming", choices=["streaming", "batch"],
                      help="Pipeline execution mode")
    parser.add_argument("--historical-path", 
                      help="Path to historical data for batch processing")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    try:
        pipeline = RapidoDataPipeline(environment=args.env)
        
        if args.mode == "streaming":
            pipeline.run_streaming_pipeline()
        elif args.mode == "batch":
            if not args.historical_path:
                print("Error: --historical-path required for batch mode", file=sys.stderr)
                sys.exit(1)
            pipeline.run_batch_pipeline(args.historical_path)
            
    except KeyboardInterrupt:
        pipeline.logger.info("Received keyboard interrupt. Shutting down...")
        pipeline.shutdown()
    except Exception as e:
        print(f"Pipeline failed: {str(e)}", file=sys.stderr)
        sys.exit(1)