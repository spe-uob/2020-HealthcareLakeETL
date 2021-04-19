from datetime import date
import pytest

from main import map_patient


class TestPatient():

    __nullable_fields = [
        "race_concept_id", "ethnicity_concept_id",
        "location_id", "care_site_id", "provider_id"
    ]
    expected_fields = [
        "person_id", "gender_concept_id",
        "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime"
    ]

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

    # def test_field_types(self, set_up):
    #     # TODO: test column data types
    #     pass

    def test_birthdate(self, spark_session):
        expect_year = 2000
        expect_month = 5
        expect_day = 17
        test_datetime = date(expect_year, expect_month, expect_day)
        columns = ["resourceType", "identifier", "birthDate", "gender"]
        data = [("Patient", "a", test_datetime, "male",)]
        rdd = spark_session.sparkContext.parallelize(data)
        df = rdd.toDF(columns)

        out = map_patient(df)
        df2 = out.first()

        assert(df2['year_of_birth'] == expect_year)
        assert(df2['month_of_birth'] == expect_month)
        assert(df2['day_of_birth'] == expect_day)
        assert(df2['birth_datetime'] == test_datetime)

    def test_gender(self, spark_session):
        # Mock data
        columns = ["resourceType", "identifier", "birthDate", "gender"]
        data = [("Patient", "a", date(2000, 2, 1), "male",),
                ("Patient", "a", date(2000, 3, 1), "female",)]
        rdd = spark_session.sparkContext.parallelize(data)
        df = rdd.toDF(columns)
        # Map data
        out = map_patient(df)
        # Compare data
        in_rows = df.collect()
        out_rows = out.collect()
        for i, x in enumerate(out_rows):
            assert(x.gender_concept_id == in_rows[i].gender)

    # def test_location(self):
    #     # TODO: test with custom location
    #     pass
