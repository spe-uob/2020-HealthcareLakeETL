from datetime import date
import pytest

from main import map_device_exposure


class TestDeviceExposure():

    __nullable_fields = [
        "device_exposure_start_datetime", "device_exposure_end_date",
        "device_exposure_end_datetime", "provider_id", "visit_occurrence_id"
    ]

    expected_fields = [
        "device_exposure_id", "person_id", "device_exposure_start_date", "device_type_concept_id"
    ]

    def test_field_names(self, data_frame):
        actual_fields = map_device_exposure(data_frame).columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(
                self.expected_fields + self.__nullable_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    # def test_date()

    # def test_time()
