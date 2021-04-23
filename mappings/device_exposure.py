import datetime as dt
import pyspark.sql.functions as F


def map_device_exposure(df):
    """ DEVICE_EXPOSURE -> DEVICE_EXPOSURE (FHIR -> OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """
    # Filter By procedure resource type

    filtered = df.filter(df['resourceType'] == 'Procedure')

    # Selects relevant fields. Using alias so we don't have columns with the same name

    Procedure = filtered.select("id",
                                F.col("encounter.reference").alias(
                                    "visit_occurrence_id"),
                                F.col("subject.reference").alias("person_id"),
                                "performedPeriod",
                                # "focalDevice",
                                "performedPeriod",
                                "performer",
                                "extension.valueCodeableConcept")

    # Extract the start date and time from the period field.
    # splits the date and time
    split_start = F.split(Procedure['performedPeriod.start'], 'T')
    split_end = F.split(Procedure['performedPeriod.end'], 'T')

    # assigns each to a column
    procedure_date_and_time = Procedure\
        .withColumn("device_exposure_start_date", split_start.getItem(0))\
        .withColumn("device_exposure_start_datetime", split_start.getItem(1))\
        .withColumn("device_exposure_end_date", split_end.getItem(0))\
        .withColumn("device_exposure_end_datetime", split_end.getItem(1))
    # Drop columns no longer needed
    dropped = procedure_date_and_time.drop("performedPeriod")

    device_exposure = dropped\
        .withColumnRenamed("id", "device_exposure_id")\
        .withColumnRenamed("valueCodeableConcept", "device_type_concept_id")\
        .withColumnRenamed("performer", "provider_id")

    # Rename the columns

    device_exposure = dropped\
        .withColumnRenamed("id", "device_exposure_id")\
        .withColumnRenamed("valueCodeableConcept", "device_type_concept_id")\
        .withColumnRenamed("performer", "provider_id")

    return device_exposure
