
  
    

        create or replace transient table tuva_synthetic.hcc_suspecting._int_prep_conditions
         as
        (

with  __dbt__cte__hcc_suspecting__stg_core__condition as (

select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , lower(normalized_code_type) as code_type
    , normalized_code as code
    , data_source
from tuva_synthetic.core.condition
), conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from __dbt__cte__hcc_suspecting__stg_core__condition

)

/*
    Default mapping guidance: Most map groups terminate with an unconditional
    rule – a rule whose predicate is “TRUE” or, equivalently, “OTHERWISE TRUE”.
    This rule is considered a “default” because it should be applied if
    nothing further is known about the patient’s condition.
*/
, seed_snomed_icd_10_map as (

    select
          referenced_component_id as snomed_code
        , map_target as icd_10_code
    from tuva_synthetic.terminology.snomed_icd_10_map
    where lower(map_rule) in ('true', 'otherwise true')
    and map_group = '1'

)

, snomed_conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , 'icd-10-cm' as code_type
        , icd_10_code as code
        , data_source
    from conditions
         inner join seed_snomed_icd_10_map
         on conditions.code = seed_snomed_icd_10_map.snomed_code
    where conditions.code_type = 'snomed-ct'

)

, other_conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from conditions
    where conditions.code_type <> 'snomed-ct'

)

, union_conditions as (

    select * from snomed_conditions
    union all
    select * from other_conditions

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as TEXT) as condition_type
        , cast(code_type as TEXT) as code_type
        , cast(code as TEXT) as code
        , cast(data_source as TEXT) as data_source
    from union_conditions

)

select
      patient_id
    , recorded_date
    , condition_type
    , code_type
    , code
    , data_source
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  