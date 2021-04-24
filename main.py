from os import environ

from mappings import (map_patient)

from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


glueContext = GlueContext(SparkContext.getOrCreate())

# catalog: database and table names
db_name = environ.get('DB_NAME')
tbl_name = environ.get('TBL_NAME')

# output s3
output_dir = environ.get('OUT_DIR')

# Create dynamic frame from source table
fhir = glueContext.create_dynamic_frame_from_catalog(
    database=db_name, table_name=tbl_name
)

# Convert to DataFrame
df = fhir.toDF()

# Perform mappings
person = DynamicFrame.fromDF(map_patient(df), glueContext, "person")

# Write Parquet to S3
glueContext.write_dynamic_frame.from_options(
    frame=person, connection_type="s3", connection_options={"path": output_dir}, format="parquet"
)
