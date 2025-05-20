# Inspired by the one-and-only Randy Pitcher and his delectable shareables https://github.com/randypitcherii/shareables/tree/main)

# Databricks notebook: Sync Unity Catalog Parquet Tables to AWS Glue

# Install necessary dependencies
# MAGIC %pip install pyarrow boto3
# MAGIC %restart_python

# Imports
import boto3
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from urllib.parse import urlparse
from pyspark.sql.utils import AnalysisException

# === USER CONFIGURATION ===
UC_CATALOG_TO_MIRROR = 'analytics_prod'             # UC catalog name to mirror
AWS_REGION           = 'us-east-1'                   # Region where Glue catalog lives
GLUE_TARGET_DATABASE = 'uc_parquet_mirror'           # Glue database to sync into

# Secrets (update scope/key names to your environment)
GLUE_ACCESS_KEY = dbutils.secrets.get("your_scope", "aws_glue_access_key")
GLUE_SECRET_KEY = dbutils.secrets.get("your_scope", "aws_glue_secret_key")

# === HELPERS ===

# Parse S3 URI into bucket and path
def parse_s3_location(location):
    parsed = urlparse(location)
    return parsed.netloc, parsed.path.lstrip("/")

# Infer Parquet schema directly from S3 path
def infer_parquet_schema_from_s3(s3_uri):
    bucket, path = parse_s3_location(s3_uri)
    s3fs = pafs.S3FileSystem(region=AWS_REGION,
                             access_key=GLUE_ACCESS_KEY,
                             secret_key=GLUE_SECRET_KEY)
    dataset = pq.ParquetDataset(path, filesystem=s3fs)
    return dataset.schema.to_arrow_schema()

# Convert Arrow types to Glue-compatible types
def parquet_field_to_glue_type(field):
    arrow_to_glue = {
        "int32": "int",
        "int64": "bigint",
        "string": "string",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
        "binary": "binary"
    }
    return arrow_to_glue.get(str(field.type), "string")

# Register or update Glue table
def register_table_to_glue(table_name, s3_location, schema):
    glue = boto3.client('glue',
                        region_name=AWS_REGION,
                        aws_access_key_id=GLUE_ACCESS_KEY,
                        aws_secret_access_key=GLUE_SECRET_KEY)

    glue_name = table_name.replace('.', '__')  # Convert UC name to Glue-safe name

    # Convert schema to Glue-compatible format
    columns = [{'Name': field.name, 'Type': parquet_field_to_glue_type(field)} for field in schema]

    # Set input/output format for Glue to recognize Parquet
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
        'Parameters': {'classification': 'parquet'}
    }

    # Create or update the Glue table
    try:
        glue.get_table(DatabaseName=GLUE_TARGET_DATABASE, Name=glue_name)
        glue.update_table(DatabaseName=GLUE_TARGET_DATABASE, TableInput=table_input)
        print(f"✅ Updated {table_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=GLUE_TARGET_DATABASE, TableInput=table_input)
        print(f"✅ Created {table_name}")

# === MAIN FUNCTION ===

def main():
    # Get list of all tables in the UC catalog
    tables = spark.sql(f"SHOW TABLES IN {UC_CATALOG_TO_MIRROR}").collect()
    for t in tables:
        try:
            full_table_name = f"{t.database}.{t.tableName}"
            desc = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]

            # Only handle Parquet tables
            if desc.format != "parquet":
                continue

            # Infer schema and register to Glue
            schema = infer_parquet_schema_from_s3(desc.location)
            register_table_to_glue(full_table_name, desc.location, schema)

        except Exception as e:
            print(f"❌ Failed for {t.tableName}: {e}")

main()
