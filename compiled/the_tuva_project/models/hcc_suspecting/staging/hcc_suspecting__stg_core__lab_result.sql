

select
      lab_result_id
    , patient_id
    , lower(coalesce(normalized_code_type,source_code_type)) as code_type
    , coalesce(normalized_code,source_code) as code
    , status
    , result
    , result_date
    , data_source
from tuva_synthetic.core.lab_result

