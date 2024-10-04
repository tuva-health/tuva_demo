
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_cqm438_denominator_criteria2
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
),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , encounter_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
),  __dbt__cte__quality_measures__stg_core__lab_result as (


select
      patient_id
    , result
    , result_date
    , collection_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.lab_result


), cholesterol_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
              'ldl cholesterol'
            , 'familial hypercholesterolemia'
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

, cholesterol_conditions as (

    select
          conditions.patient_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join cholesterol_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = cholesterol_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = cholesterol_codes.code

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, cholesterol_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date as evidence_date
    from procedures
         inner join cholesterol_codes
             on procedures.code = cholesterol_codes.code
             and procedures.code_type = cholesterol_codes.code_system

)

, labs as (

    select
          patient_id
        , result
        , result_date
        , collection_date
        , source_code_type
        , source_code
        , normalized_code_type
        , normalized_code
    from __dbt__cte__quality_measures__stg_core__lab_result

)

, cholesterol_tests_with_result as (

    select
      labs.patient_id
    , labs.result as evidence_value
    , coalesce(collection_date, result_date) as evidence_date
    , cholesterol_codes.concept_name
    , row_number() over(partition by labs.patient_id order by
                          labs.result desc
                        , result_date desc) as rn
    from labs
    inner join cholesterol_codes
      on coalesce(labs.normalized_code, labs.source_code) = cholesterol_codes.code
        and coalesce(labs.normalized_code_type, labs.source_code_type) = cholesterol_codes.code_system
   
        where regexp_like(result, '^[+-]?([0-9]*[.])?[0-9]+$')
    

)

, cholesterol_labs as (

    select
          patient_id
        , evidence_date
    from cholesterol_tests_with_result
    where rn= 1
        and cast(evidence_value as numeric(28,6)) >= 190

)

, all_patients_with_cholesterol as (

    select
          cholesterol_conditions.patient_id
        , cholesterol_conditions.evidence_date
    from cholesterol_conditions

    union all

    select
          cholesterol_procedures.patient_id
        , cholesterol_procedures.evidence_date
    from cholesterol_procedures

    union all

    select
          cholesterol_labs.patient_id
        , cholesterol_labs.evidence_date
    from cholesterol_labs

)

, patients_with_cholesterol as (

    select
        distinct
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from all_patients_with_cholesterol
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
    from patients_with_cholesterol

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
      
  