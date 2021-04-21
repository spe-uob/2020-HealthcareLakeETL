from datetime import date
import pytest

from main import map_device_exposure

# --------------------------- NOT FINISHED ------------------------#


class TestDeviceExposure():

    __nullable_fields = [
        "device_exposure_start_datetime", "device_exposure_end_date",
        "device_exposure_end_datetime", "unique_device_id", "quantity", "provider_id", "visit_occurrence_id", "visit_detail_id", "device_source_value"
    ]

    expected_fields = [
        "device_exposure_id", "person_id",
        "device_concept_id", "device_exposure_start_date", "device_type_concept_id", "",
        "device_source_concept_id"
    ]

    @pytest.fixture()
    def set_up(self, data_frame):
        # Using data/catalog.parquet for tests
        yield map_device_exposure(data_frame)

    def test_field_names(self, set_up):
        actual_fields = set_up.columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(self.expected_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    # def test_date()

    # def test_time()
