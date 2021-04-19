import datetime as dt
import pyspark.sql.functions as F


def map_observation(df):
    """ Observation -> Observation (FHIR -> OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """

    filtered = df.filter(df['resourceType'] == 'Observation')
    Observation = filtered.filter(filtered.valueCodeableConcept.isNotNull())
    Observation = Observation.select(['id',
                                'subject',
                                'code',
                                'performer',
                                'encounter',
                                'meta',
                                'effectiveDateTime',
                                'valueCodeableConcept',
                                'category'])

    split_dates = F.split(Observation["effectiveDateTime"], 'T')

    Observation = Observation.withColumnRenamed("id", "observation_id")\
                            .withColumn("observation_type_concept_id", Observation.category.coding.getItem(0).code.getItem(0))\
                            .withColumn("observation_date", split_dates.getItem(0))\
                            .withColumn("person_id", Observation.subject.reference)\
                            .withColumn("value_as_string", Observation.valueCodeableConcept.text)\
                            .withColumnRenamed("code", "observation_concept_id")\
                            .withColumnRenamed("effectiveDateTime", "observation_datetime")\
                            .drop("valueCodeableConcept")\
                            .withColumn("visit_occurrence_id", Observation.encounter.reference)\
                            .withColumnRenamed("performer", "provider_id")\
                            .drop("encounter")\
                            .drop("subject")\
                            .drop("meta")\
                            .drop("category")

    Observation = Observation.withColumn("observation_concept_id", Observation.observation_concept_id.coding.getItem(0).code)
    return Observation
