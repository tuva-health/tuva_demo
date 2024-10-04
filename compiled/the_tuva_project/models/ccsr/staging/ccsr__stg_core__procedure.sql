

select 
      encounter_id
    , claim_id
    , patient_id
    , normalized_code
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure