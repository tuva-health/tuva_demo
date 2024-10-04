
select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , lower(normalized_code_type) as code_type
    , normalized_code as code
    , data_source
from tuva_synthetic.core.condition