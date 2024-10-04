

select 
      patient_id
    , normalized_code
    , recorded_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.condition