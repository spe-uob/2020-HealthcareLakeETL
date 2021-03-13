# HealthcareLakeETL

This repository contains the Spark ETL jobs for our AWS Glue pipeline. Used by the [HealthcareLake](https://github.com/spe-uob/HealthcareLake) project.

### FHIR -> OMOP

We are transforming one dataframe into several dataframes. The exact mapping can be found here:

[Mappings](https://build.fhir.org/ig/HL7/cdmh/profiles.html#omop-to-fhir-mappings)

There are open issues for each. Your task is to work out how to do the mapping and submit a Jupyter Notebook for your experiment.

## Local development

These instructions are for working with the data offline as opposed to connecting to AWS EMR. This is recommended as there is less setup involved.

To setup the Jupyter Notebook environment, follow these steps:

1. Install Anaconda

2. Create a Virtual Environment with Anaconda

```
conda create --name etl python=3.9
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
