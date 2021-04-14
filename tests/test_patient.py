import pytest

from main import map_patient


class TestPatient():

    __nullable_fields = [
        "race_concept_id", "ethnicity_concept_id",
        "location_id", "care_site_id"
    ]
    expected_fields = [
        "person_id", "provider_id", "gender_concept_id",
        "year_of_birth", "month_of_birth", "day_of_birth", "birth_datetime"
    ] + __nullable_fields

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

    def test_birthdate(self):
        # TODO: test with a custom dataframe
        pass

    def test_gender(self):
        # TODO: test with custom gender dataframe
        pass

    def test_location(self):
        # TODO: test with custom location
        pass
