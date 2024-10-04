

select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.encounter

