
    
    



with __dbt__cte__quality_measures__stg_core__observation as (


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


) select patient_id
from __dbt__cte__quality_measures__stg_core__observation
where patient_id is null


