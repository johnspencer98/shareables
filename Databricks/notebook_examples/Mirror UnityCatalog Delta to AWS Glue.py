# Databricks notebook: Sync Unity Catalog Delta Tables to AWS Glue

# Install delta + boto3
# MAGIC %pip install delta-spark boto3
# MAGIC %restart_python

# Setup Spark with Delta support
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import boto3

builder = SparkSession.builder.appName("DeltaToGlueSync")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# === USER CONFIGURATION ===
UC_CATALOG_TO_MIRROR = 'analytics_prod'
GLUE_TARGET_DATABASE = 'uc_delta_mirror'
AWS_REGION           = 'us-east-1'

# Secrets (update to match your setup)
GLUE_ACCESS_KEY = dbutils.secrets.get("your_scope", "aws_glue_access_key")
GLUE_SECRET_KEY = dbutils.secrets.get("your_scope", "aws_glue_secret_key")

# === HELPERS ===

# Map Spark types to Glue-compatible types
def spark_type_to_glue_type(dtype):
    mapping = {
        "IntegerType": "int",
        "LongType": "bigint",
        "DoubleType": "double",
        "FloatType": "float",
        "StringType": "string",
        "BooleanType": "boolean",
        "BinaryType": "binary"
    }
    return mapping.get(dtype, "string")

# Register or update a Glue table for Delta format
def register_delta_table_to_glue(table_name, s3_location, schema):
    glue = boto3.client('glue',
                        region_name=AWS_REGION,
                        aws_access_key_id=GLUE_ACCESS_KEY,
                        aws_secret_access_key=GLUE_SECRET_KEY)

    glue_name = table_name.replace('.', '__')

    columns = [{'Name': f.name, 'Type': spark_type_to_glue_type(f.dataType.simpleString())}
               for f in schema.fields]

    input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

    table_input = {
        'Name': glue_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': {'SerializationLibrary': serde},
            'StoredAsSubDirectories': False
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'classification': 'delta'}
    }

    try:
        glue.get_table(DatabaseName=GLUE_TARGET_DATABASE, Name=glue_name)
        glue.update_table(DatabaseName=GLUE_TARGET_DATABASE, TableInput=table_input)
        print(f"✅ Updated {table_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=GLUE_TARGET_DATABASE, TableInput=table_input)
        print(f"✅ Created {table_name}")

# === MAIN FUNCTION ===

def main():
    # Get all tables in the UC catalog
    tables = spark.sql(f"SHOW TABLES IN {UC_CATALOG_TO_MIRROR}").collect()
    for t in tables:
        try:
            full_table_name = f"{t.database}.{t.tableName}"
            desc = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]

            # Skip non-Delta tables
            if desc.format != "delta":
                continue

            # Read schema and register
            df = spark.read.format("delta").load(desc.location)
            register_delta_table_to_glue(full_table_name, desc.location, df.schema)

        except Exception as e:
            print(f"❌ Failed for {t.tableName}: {e}")

main()
