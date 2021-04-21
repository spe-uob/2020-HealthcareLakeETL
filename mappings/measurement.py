import datetime as dt
import pyspark.sql.functions as F

def map_measurement(df):
    """ Observation -> Measurement (FHIR -> OMOP)

    :param df: Input frame of FHIR records
    :type df: DynamicFrame
    :return: Output frame of OMOP transform
    :rtype: DynamicFrame
    """

    filtered = df.filter(df['resourceType'] == 'Observation')
    Measurement = filtered.filter(filtered.valueCodeableConcept.isNull())
    Measurement = Measurement.select(['id',
                            'subject',
                            'code',
                            'performer',
                            'encounter',
                            'meta',
                            'category',      
                            'valueQuantity',
                            'effectiveDateTime',
                            'Extension.valueCodeableConcept',
                            'component'])

    split_dates = F.split(Measurement["effectiveDateTime"], 'T')
    val_as_num = F.coalesce(F.col("value_as_number.double"), F.col("value_as_number.long"))

    Measurement = Measurement.withColumnRenamed("id", "measurement_id")\
                            .withColumn("measurement_date", split_dates.getItem(0))\
                            .withColumn("person_id", Measurement.subject.reference)\
                            .drop("subject")\
                            .withColumnRenamed("code", "measurement_concept_id")\
                            .withColumnRenamed("effectiveDateTime", "measurement_datetime")\
                            .drop("valueCodeableConcept")\
                            .withColumn("measurement_type_concept_id", Measurement.category.getItem(0).coding.code.getItem(0))\
                            .withColumn("value_as_number", Measurement.valueQuantity.value)\
                            .withColumn("visit_occurrence_id", Measurement.encounter.reference)\
                            .drop("encounter")\
                            .withColumn("value_as_number", val_as_num)\
                            .withColumn("unit_source_value", Measurement.valueQuantity.unit)\
                            .withColumnRenamed("performer", "provider_id")\
                            .drop("valueQuantity")\
                            .drop("meta")



    Measurement = Measurement.withColumn("measurement_concept_id", Measurement.measurement_concept_id.coding.getItem(0).code)
                        
    
    Measurement = Measurement.withColumn("distolic", Measurement.component.getItem(0).valueQuantity.value)
    Measurement = Measurement.withColumn("systolic", Measurement.component.getItem(1).valueQuantity.value)
    Measurement = Measurement.withColumn("value_as_num_combine", F.when(F.col("distolic") >0 ,F.array("systolic", "distolic")))\
                            .withColumn("value_as_number", F.array(Measurement.value_as_number))

    Measurement = Measurement.withColumn("value_as_number", F.coalesce(Measurement.value_as_num_combine, Measurement.value_as_number))\
                            .drop("distolic", "systolic", "value_as_num_combine", "component","category")
            
    return Measurement              
