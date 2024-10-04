

select
      patient_id
    , encounter_id
    , observation_date
    , result
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , normalized_description
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.observation

