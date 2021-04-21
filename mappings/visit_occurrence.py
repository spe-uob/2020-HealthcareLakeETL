import datetime as dt
from pyspark.sql.functions import dayofmonth, month, year, to_date, trunc, split, explode, array


def map_visit_occurrence(df):
    """ VISIT_OCCURRENCE -> VISIT_OCCURRENCE (FHIR -> OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """
    # Filter By Encounter Resource type
    filtered = df.filter(df['resourceType'] == 'Encounter')
    Encounter = filtered.select(['id', 'subject', 'type',
                                'location', 'hospitalization.admitSource',
                                 'period', 'extension.valueCodeableConcept'])
    # splits the date and time
    split_start = split(Encounter['period.start'], 'T')
    split_end = split(Encounter['period.end'], 'T')
    # assigns each to a column
    visit_date_time = Encounter\
        .withColumn("visit_start_date", split_start.getItem(0))\
        .withColumn("visit_start_datetime", split_start.getItem(1))\
        .withColumn("visit_end_date", split_end.getItem(0))\
        .withColumn("visit_end_datetime", split_end.getItem(1))
    # Drop columns no longer needed
    dropped = visit_date_time.drop("period")
    # Rename the columns
    visit_occurrence = dropped\
        .withColumnRenamed("type", "preceding_visit_occurrence")\
        .withColumnRenamed("id", "visit_occurrence_id")\
        .withColumnRenamed("admitSource", "admitting_source_concept_id")\
        .withColumnRenamed("subject", "person_id")\
        .withColumnRenamed("type", "preceding_visit_occurrence")\
        .withColumnRenamed("location", "care_site_id")\
        .withColumnRenamed("valueCodeableConcept", "visit_type_concept_id")
    # .withColumnRenamed("location.location.id","care_site_id")\
    # .withColumnRenamed("location.location.type","discharge_to_concept_id")\

    return visit_occurrence
