
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_cqm438_denominator_criteria3
         as
        (

with  __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , encounter_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.condition
), diabetes_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
              'diabetes'
        )

)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from __dbt__cte__quality_measures__stg_core__condition

)

, diabetes_conditions as (

    select
          conditions.patient_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join diabetes_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = diabetes_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = diabetes_codes.code

)

, patients_with_diabetes as (

    select
        distinct
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version 
    from diabetes_conditions
    inner join tuva_synthetic.quality_measures._int_cqm438__performance_period pp
    on evidence_date <= pp.performance_period_end

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
    from patients_with_diabetes

)

select 
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  