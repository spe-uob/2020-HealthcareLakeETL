import pytest

from main import map_patient


class TestPatient():

    @pytest.fixture()
    def set_up(self, data_frame):
        yield map_patient(data_frame)

    def test_fields(self, set_up):
        expected_fields = [
            "person_id", "provider_id", "care_site_id",
            "gender_concept_id", "year_of_birth", "month_of_birth",
            "day_of_birth", "birth_datetime", "race_concept_id",
            "ethnicity_concept_id", "location_id"
        ]
        actual_fields = set_up.columns
        # Test whether column names exist in dataframe
        assert(set(actual_fields) == set(expected_fields)
               ), "Resulting columns did not match the expected columns: %s" % expected_fields
