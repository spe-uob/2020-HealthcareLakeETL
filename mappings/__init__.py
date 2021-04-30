from .patient import map_patient
from .observation import map_observation
from .measurement import map_measurement
from .visit_occurrence import map_visit_occurrence
from .procedure_occurrence import map_procedure_occurrence
from .device_exposure import map_device_exposure

mappings = {
    "Person": map_patient,
    "OBSERVATION": map_observation,
    "MEASUREMENT": map_measurement,
    "VISIT_OCCURENCE": map_visit_occurrence,
    "PROCEDURE_OCCURENCE": map_procedure_occurrence,
    "DEVICE_EXPOSURE": map_device_exposure
}
