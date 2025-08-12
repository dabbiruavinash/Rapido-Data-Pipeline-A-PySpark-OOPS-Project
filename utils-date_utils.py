from pyspark.sql import functions as F
from pyspark.sql import DataFrame

class DateUtils:
    """Utilities for date/time operations"""
    
    @staticmethod
    def add_time_periods(df: DataFrame, timestamp_col: str) -> DataFrame:
        """Add common time period columns to DataFrame"""
        return df \
            .withColumn("year", F.year(timestamp_col)) \
            .withColumn("month", F.month(timestamp_col)) \
            .withColumn("day", F.dayofmonth(timestamp_col)) \
            .withColumn("hour", F.hour(timestamp_col)) \
            .withColumn("day_of_week", F.dayofweek(timestamp_col)) \
            .withColumn("week_of_year", F.weekofyear(timestamp_col))
            
    @staticmethod
    def filter_time_range(df: DataFrame, timestamp_col: str, start_date: str, end_date: str) -> DataFrame:
        """Filter DataFrame by date range"""
        return df.filter(
            (F.col(timestamp_col) >= F.to_timestamp(F.lit(start_date))) &
            (F.col(timestamp_col) <= F.to_timestamp(F.lit(end_date)))
        )