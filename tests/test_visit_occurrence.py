from datetime import date
import pytest
#import pyspark.sql.functions as F
#import re

from main import map_visit_occurrence

# --------------------------- NOT FINISHED ------------------------#


class TestVisitOccurrence():

    __nullable_fields = [
        "visit_start_date", "visit_end_date",
        "provider_id", "care_site_id"
    ]

    expected_fields = [
        "visit_occurrence_id", "person_id",
        "visit_concept_id", "visit_start_datetime", "visit_end_datetime", "visit_type_concept_id",
        "admitted_from_concept_id"
    ]

    # @pytest.fixture()
    # def set_up(self, data_frame):
    #     # Using data/catalog.parquet for tests
    #     yield map_visit_occurrence(data_frame)

    # def test_field_names(self, set_up):
    #     actual_fields = set_up.columns
    #     # Test whether column names exist in dataframe
    #     assert(
    #         set(actual_fields) == set(self.expected_fields)
    #     ), \
    #         "Resulting columns did not match the expected columns: %s"\
    #         % self.expected_fields

    def test_field_names(self, data_frame):
        actual_fields = map_visit_occurrence(data_frame).columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(
                self.expected_fields + self.__nullable_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    # def test_date(self, dataframe):
    #     # test the start and end date
    #     # filter out nullable fields
    #     start_value = []
    #     end_value = []
    #     filtered_start = dataframe.where(F.col('visit_start_date').isNotNull())
    #     start_value[0] = filtered_start.select(1)
    #     start_value[1] = filtered_start.tail(1)
    #     filtered_end = dataframe.where(F.col('visit_end_date').isNotNull())
    #     end_value[0] = filtered_end.select(1)
    #     end_value[1] = filtered_end.select(1)
    #     for i in range(2):
    #         assert re.match(
    #             "^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", start_value[i])
    #         assert re.match(
    #             "^\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])$", end_value[i])

    # def test_time():
        # test time
