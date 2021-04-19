from datetime import date
import pytest
from main import map_observation

class TestObservation():

    __nullable_fields = [
        "observation_datetime", "value_as_string", "provider_id",
        "visit_occurrence_id"
    ]
    expected_fields = [ 
        "observation_id", "person_id", "observation_concept_id",
        "observation_date", "observation_type_concept_id",
    ]

    @pytest.fixture()
    def set_up(self, data_frame):
        # Using data/catalog.parquet for tests
        yield map_observation(data_frame)

    def test_field_names(self, set_up):
        actual_fields = set_up.columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(self.expected_fields)
        ) , \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields
        
    # def test_datetime(self, spark_session):
    #     TODO