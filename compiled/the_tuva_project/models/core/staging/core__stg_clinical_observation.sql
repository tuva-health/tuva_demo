


select
    cast(observation_id as TEXT ) as observation_id
    , cast(patient_id as TEXT ) as patient_id
    , cast(encounter_id as TEXT ) as encounter_id
    , cast(panel_id as TEXT ) as panel_id
    , try_cast( observation_date as date ) as observation_date
    , cast(observation_type as TEXT ) as observation_type
    , cast(source_code_type as TEXT ) as source_code_type
    , cast(source_code as TEXT ) as source_code
    , cast(source_description as TEXT ) as source_description
    , cast(normalized_code_type as TEXT ) as normalized_code_type
    , cast(normalized_code as TEXT ) as normalized_code
    , cast(normalized_description as TEXT ) as normalized_description
    , cast(result as TEXT ) as result
    , cast(source_units as TEXT ) as source_units
    , cast(normalized_units as TEXT ) as normalized_units
    , cast(source_reference_range_low as TEXT ) as source_reference_range_low
    , cast(source_reference_range_high as TEXT ) as source_reference_range_high
    , cast(normalized_reference_range_low as TEXT ) as normalized_reference_range_low
    , cast(normalized_reference_range_high as TEXT ) as normalized_reference_range_high
    , cast(data_source as TEXT ) as data_source
    , cast('2024-10-04 19:11:18.274664+00:00' as TIMESTAMP ) as tuva_last_run
from tuva_synthetic.input_layer.observation