import sys

from mappings import (map_patient)

from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ["DB_NAME", "TBL_NAME", "OUT_DIR"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name="FHIR->OMOP", args=args)

# catalog: database and table names
db_name = args["DB_NAME"]
tbl_name = args['TBL_NAME']

# output s3
output_dir = args['OUT_DIR']

# Create dynamic frame from source table
datasource = glueContext.create_dynamic_frame_from_catalog(
    database=db_name, table_name=tbl_name, transformation_ctx="datasource"
)

# Convert to DataFrame
df = datasource.toDF()

# Perform mappings
person = DynamicFrame.fromDF(map_patient(df), glueContext, "person")

# Write Parquet to S3
datasink = glueContext.write_dynamic_frame.from_options(
    frame=person, connection_type="s3", connection_options={"path": output_dir},
    format="parquet", transformation_ctx="datasink"
)

job.commit()
