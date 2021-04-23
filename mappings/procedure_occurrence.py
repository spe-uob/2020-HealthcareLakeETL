import datetime as dt
import pyspark.sql.functions as F


def map_procedure_occurrence(df):
    """ PROCEDURE_OCCURRENCE -> PROCEDURE_OCCURRENCE (FHIR -> OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """
    # Filter By Procedure Resource type

    filtered = df.filter(df['resourceType'] == 'Procedure')

    # Selects relevant fields. Using alias so we don't have columns with the same name

    Procedure = filtered.select("id",
                                F.col("encounter.reference").alias(
                                    "visit_occurrence_id"),
                                F.col("subject.reference").alias("person_id"),
                                "code.coding",
                                "performedPeriod",
                                "performer",
                                "extension")

    # Extract the start date and time from the period field.
    # splits the date and time
    split_start = F.split(Procedure['performedPeriod.start'], 'T')

    # assigns each to a column
    procedure_date_and_time = Procedure\
        .withColumn("procedure_date", split_start.getItem(0))\
        .withColumn("procedure_datetime", split_start.getItem(1))
    # Drop columns no longer needed
    dropped = procedure_date_and_time.drop("performedPeriod")

    # Rename the columns
    procedure_occurrence = dropped\
        .withColumnRenamed("id", "procedure_occurrence_id")\
        .withColumnRenamed("coding", "procedure_concept_id")\
        .withColumnRenamed("extension", "procedure_type_concept_id")\
        .withColumnRenamed("performer", "provider_id")
    return procedure_occurrence
