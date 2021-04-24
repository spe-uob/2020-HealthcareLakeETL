from mappings import map_procedure_occurrence


class TestProcedureOccurrence():

    __nullable_fields = [
        "procedure_date",
        "provider_id", "visit_occurrence_id"
    ]

    expected_fields = [
        "procedure_occurrence_id", "person_id",
        "procedure_concept_id", "procedure_datetime", "procedure_type_concept_id"
    ]

    def test_field_names(self, data_frame):
        actual_fields = map_procedure_occurrence(data_frame).columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(
                self.expected_fields + self.__nullable_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    # def test_date()

    # def test_time()
