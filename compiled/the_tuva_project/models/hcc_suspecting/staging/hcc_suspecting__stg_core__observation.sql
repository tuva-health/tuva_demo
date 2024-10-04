

select
      patient_id
    , observation_date
    , result
    , lower(coalesce(normalized_code_type,source_code_type)) as code_type
    , coalesce(normalized_code,source_code) as code
    , data_source
from tuva_synthetic.core.observation

