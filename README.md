# HealthcareLakeETL

Spark ETL scripts for AWS Glue

### FHIR -> OMOP

[Mappings](https://build.fhir.org/ig/HL7/cdmh/profiles.html#omop-to-fhir-mappings)

## Development

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
In your notebook
```python
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('etl').getOrCreate()

df = spark.read.parquet('data/catalog.parquet')
```

That's it, you have the DataFrame to work with.
