from datetime import datetime
import pytest

from main import map_patient

from pyspark.sql.types import DateType, StructType, StructField, StringType, IntegerType


class TestPatient():

    __nullable_fields = [
        "race_concept_id", "ethnicity_concept_id",
        "location_id", "care_site_id"
    ]
    expected_fields = [
        "person_id", "provider_id", "gender_concept_id",
        "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime"
    ] + __nullable_fields

    schema = StructType([
        StructField("person_id", StringType(), False),
        StructField("provider_id", StringType(), True),
        StructField("gender_concept_id", StringType(), True),
        StructField("year_of_birth", IntegerType(), True),
        StructField("month_of_birth", IntegerType(), True),
        StructField("day_of_birth", IntegerType(), True),
        StructField("birth_datetime", DateType(), True),
    ])

    @pytest.fixture()
    def set_up(self, data_frame):
        # Using data/catalog.parquet for tests
        yield map_patient(data_frame)

    def test_field_names(self, set_up):
        actual_fields = set_up.columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(self.expected_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    def test_field_types(self, set_up):
        # TODO: test column data types
        pass

    def test_birthdate(self, spark_session):
        expect_year = 2000
        expect_month = 5
        expect_day = 17
        test_datetime = datetime(
            expect_year, expect_month, expect_day, hour=6, minute=10, second=5
        )
        data = [(test_datetime,)]
        rdd = spark_session.sparkContext.parallelize(data)
        df = rdd.toDF(["birthDate"])

        out = map_patient(df)
        df2 = out.first()

        assert(df2['year_of_birth'] == expect_year)
        assert(df2['month_of_birth'] == expect_month)
        assert(df2['day_of_birth'] == expect_day)
        assert(df2['birth_datetime'] == test_datetime)

    def test_gender(self, spark_session):
        # Mock data
        columns = ["gender"]
        data = [("male",), ("female",)]
        rdd = spark_session.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        # Map data
        out = map_patient(df)
        # Compare data
        in_rows = df.collect()
        out_rows = out.collect()
        for i, x in enumerate(out_rows):
            assert(x.gender_concept_id == in_rows[i].gender)

    def test_location(self):
        # TODO: test with custom location
        pass
