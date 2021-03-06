import pytest

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder\
        .appName('testing')\
        .config('spark.driver.bindAddress', '127.0.0.1')\
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def data_frame(spark_session):
    return spark_session.read.parquet('data/catalog.parquet')
