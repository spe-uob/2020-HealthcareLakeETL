from datetime import date
import pytest


from main import map_procedure_occurrence

# --------------------------- NOT FINISHED ------------------------#


class TestProcedureOccurrence():

    __nullable_fields = [
        "procedure_date", "quantity",
        "provider_id", "visit_occurrence_id", "visit_detail_id", "procedure_source_value", "modifier_source_value"
    ]

    expected_fields = [
        "procedure_occurrence_id", "person_id",
        "procedure_concept_id", "procedure_datetime", "procedure_type_concept_id", "modifier_concept_id",
        "procedure_source_concept_id"
    ]

    @pytest.fixture()
    def set_up(self, data_frame):
        # Using data/catalog.parquet for tests
        yield map_procedure_occurrence(data_frame)

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
