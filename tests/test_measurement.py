from mappings import map_measurement


class TestMeasurement():

    __nullable_fields = [
        "measurement_datetime", "value_as_number", "provider_id",
        "visit_occurrence_id", "unit_source_value"
    ]
    expected_fields = [
        "measurement_id", "person_id", "measurement_concept_id",
        "measurement_date", "measurement_type_concept_id",
        "measurement_datetime", "value_as_number", "provider_id",
        "visit_occurrence_id", "unit_source_value"
    ]

    def test_field_names(self, data_frame):
        actual_fields = map_measurement(data_frame).columns
        # Test whether column names exist in dataframe
        assert(
            set(actual_fields) == set(self.expected_fields)
        ), \
            "Resulting columns did not match the expected columns: %s"\
            % self.expected_fields

    # def test_datetime(self, spark_session):
    #     TODO

    # def test_blood_pressures(self, spark_session):
    #     TODO
