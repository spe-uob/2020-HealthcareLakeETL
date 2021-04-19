from pyspark.sql.functions import dayofmonth, month, year, to_date


def map_patient(df):
    """Patient->Person (FHIR->OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """
    patients = df.filter(df['resourceType'] == 'Patient')
    persons = patients.select(['identifier', 'gender', 'birthDate'])
    stage_persons = persons\
        .withColumn("year_of_birth", year(persons['birthDate']))\
        .withColumn("month_of_birth", month(persons['birthDate']))\
        .withColumn("day_of_birth", dayofmonth(persons['birthDate']))\
        .withColumnRenamed("birthDate", "birth_datetime")

    patient_dataframe = stage_persons.withColumnRenamed("identifier", "person_id")\
        .withColumnRenamed("gender", "gender_concept_id")

    return patient_dataframe
