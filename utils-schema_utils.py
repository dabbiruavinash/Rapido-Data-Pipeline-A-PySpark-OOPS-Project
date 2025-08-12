from pyspark.sql import functions as F
from pyspark.sql.types import StructType

class SchemaUtils:
    """Utilities for working with schemas and data validation"""
    
    @staticmethod
    def from_json(col: str, schema: StructType):
        """Parse JSON column with given schema"""
        return F.from_json(col, schema)
        
    @staticmethod
    def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
        """Ensure DataFrame matches expected schema"""
        # Check for missing columns and add them with null values
        for field in schema.fields:
            if field.name not in df.columns:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                
        # Select only the columns in the schema in the correct order
        return df.select(*[field.name for field in schema.fields])
        
    @staticmethod
    def validate_schema(df: DataFrame, schema: StructType) -> bool:
        """Validate that DataFrame schema matches expected schema"""
        df_schema = df.schema
        if len(df_schema) != len(schema):
            return False
            
        for i in range(len(schema)):
            df_field = df_schema[i]
            schema_field = schema.fields[i]
            
            if df_field.name != schema_field.name or df_field.dataType != schema_field.dataType:
                return False
                
        return True