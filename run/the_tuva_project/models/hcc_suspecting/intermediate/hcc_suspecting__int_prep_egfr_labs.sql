
  
    

        create or replace transient table tuva_synthetic.hcc_suspecting._int_prep_egfr_labs
         as
        (

with  __dbt__cte__hcc_suspecting__stg_core__lab_result as (


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


), lab_result as (

    select
          patient_id
        , data_source
        , code_type
        , code
        , status
        , result
        , result_date
    from __dbt__cte__hcc_suspecting__stg_core__lab_result

)

, seed_egfr_codes as (

    select
          concept_name
        , code
        , code_system
    from tuva_synthetic.hcc_suspecting._value_set_clinical_concepts
    where lower(concept_name) = 'estimated glomerular filtration rate'

)

, egfr_labs as (

    select distinct
          lab_result.patient_id
        , lab_result.data_source
        , lab_result.code_type
        , lab_result.code
        , lab_result.result_date
        , lab_result.result
    from lab_result
        inner join seed_egfr_codes
        on lab_result.code = seed_egfr_codes.code
        and lab_result.code_type = seed_egfr_codes.code_system
    where lab_result.result is not null
    and lower(lab_result.status) not in ('cancelled', 'entered-in-error')

)

, numeric_egfr_labs as (

    select
          patient_id
        , data_source
        , code_type
        , code
        , result_date
        , cast(result as numeric(28,6)) as result
    from egfr_labs
   
        where regexp_like(result, '^[+-]?([0-9]*[.])?[0-9]+$')
    

)

, clean_non_numeric_egfr_labs as (

    select
          patient_id
        , data_source
        , code_type
        , code
        , result_date
        , result
        , cast(case
            when lower(result) like '%unsatisfactory specimen%' then null
            when result like '%>%' then null
            when result like '%<%' then null
            when result like '%@%' then trim(replace(result,'@',''))
            when result like '%mL/min/1.73m2%' then trim(replace(result,'mL/min/1.73m2',''))
            else null
          end as numeric(28,6)) as clean_result
    from egfr_labs
    
        where regexp_like(result, '^[+-]?([0-9]*[.])?[0-9]+$') = False
    

)

, unioned_labs as (

    select
          patient_id
        , data_source
        , code_type
        , code
        , result_date
        , result
    from numeric_egfr_labs

    union all

    select
          patient_id
        , data_source
        , code_type
        , code
        , result_date
        , clean_result as result
    from clean_non_numeric_egfr_labs
    where clean_result is not null

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(data_source as TEXT) as data_source
        , cast(code_type as TEXT) as code_type
        , cast(code as TEXT) as code
        , cast(result_date as date) as result_date
        , cast(result as numeric(28,6)) as result
    from unioned_labs

)

select
      patient_id
    , data_source
    , code_type
    , code
    , result_date
    , result
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  