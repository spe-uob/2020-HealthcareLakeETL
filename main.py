import sys

from mappings import mappings

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


for name, mapping in mappings.items():
    # Perform mapping
    dyf = DynamicFrame.fromDF(mapping(df), glueContext, mapping.__qualname__)
    # Write table to S3
    glueContext.write_dynamic_frame.from_options(
        frame=dyf, connection_type="s3", format="parquet",
        connection_options={"path": output_dir+name}
    )


job.commit()
