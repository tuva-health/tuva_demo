

select
    cast(condition_id as TEXT ) as condition_id
    , cast(patient_id as TEXT ) as patient_id
    , cast(encounter_id as TEXT ) as encounter_id
    , cast(claim_id as TEXT ) as claim_id
    , try_cast( recorded_date as date ) as recorded_date
    , try_cast( onset_date as date ) as onset_date
    , try_cast( resolved_date as date ) as resolved_date
    , cast(status as TEXT ) as status
    , cast(condition_type as TEXT ) as condition_type
    , cast(source_code_type as TEXT ) as source_code_type
    , cast(source_code as TEXT ) as source_code
    , cast(source_description as TEXT ) as source_description
    , cast(normalized_code_type as TEXT ) as normalized_code_type
    , cast(normalized_code as TEXT ) as normalized_code
    , cast(normalized_description as TEXT ) as normalized_description
    , cast(condition_rank as INT ) as condition_rank
    , cast(present_on_admit_code as TEXT ) as present_on_admit_code
    , cast(present_on_admit_description as TEXT ) as present_on_admit_description
    , cast(data_source as TEXT ) as data_source
    , cast('2024-10-04 19:11:18.274664+00:00' as TIMESTAMP ) as tuva_last_run
from tuva_synthetic.input_layer.condition