[![Pytest](https://github.com/spe-uob/HealthcareLakeETL/actions/workflows/python-app.yml/badge.svg)](https://github.com/spe-uob/HealthcareLakeETL/actions/workflows/python-app.yml)

# HealthcareLakeETL

This repository contains the Spark ETL jobs for our AWS Glue pipeline. Used by the [HealthcareLake](https://github.com/spe-uob/HealthcareLake) project.

### FHIR &rarr; OMOP

We are transforming one dataframe (FHIR) into several dataframes that correspond with the OMOP Common Data Model (CDM). The exact mapping can be found [here](https://build.fhir.org/ig/HL7/cdmh/profiles.html#omop-to-fhir-mappings).

Once the patient-level data model (FHIR) has been transformed to the population-level data model (OMOP CDM), we can access the Observational Health Data Sciences and Informatics (OHDSI) resources that can perform data aggregations and packages for cohort creation and various population level data analytics. [More info](https://www.ohdsi.org/data-standardization/)


There are open issues for each. Your task is to work out how to do the mapping and submit a Jupyter Notebook for your experiment. We will combine the notebook experiments into one Python script at the end and this will be run as one automated job on AWS Glue.

## Testing

```
pytest
```

## Local development

These instructions are for working with the data offline as opposed to connecting to AWS EMR. This is recommended as there is less setup involved.

To setup the Jupyter Notebook environment, follow these steps:

1. Install Anaconda

2. Create a Virtual Environment with Anaconda

```
conda create --name etl python=3.7
```

3. Switch to this virtual environment

```
conda activate etl
```

4. Add the environment to jupyter kernels

```
pip install --user ipykernel
```
And then link it
```
python -m ipykernel install --user --name=etl
```

You should now be able to run jupyter notebook in your browser:
```
jupyter notebook
```
Select Kernel&rarr;Change kernel&rarr;etl

5. Install PySpark

Open a new terminal. (Remember to activate the environment with `conda activate etl`)
```
pip install pyspark
```

6. Start developing

In your notebook:
```python
from pyspark.sql import SparkSession

# Create a local Spark session
spark = SparkSession.builder.appName('etl').getOrCreate()

# Read in our data
df = spark.read.parquet('data/catalog.parquet')
```

That's it, you have the DataFrame to work with.
