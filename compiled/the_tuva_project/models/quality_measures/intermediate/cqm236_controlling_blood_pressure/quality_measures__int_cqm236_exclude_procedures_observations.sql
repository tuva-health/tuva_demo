

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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), denominator as (

    select
          patient_id
        , age
        , performance_period_begin
        , performance_period_end
    from tuva_synthetic.quality_measures._int_cqm236_denominator

)


, conditions as (

    select
          patient_id
        , claim_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__condition

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
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
          'dialysis services'
        , 'end stage renal disease'
        , 'esrd monthly outpatient services'
        , 'kidney transplant'
        , 'kidney transplant recipient'
        , 'pregnancy'
    )

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

)

, exclusions_unioned as (

    select
          patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select
          patient_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

)

, excluded_patients as (

    select
          exclusions_unioned.patient_id
        , exclusions_unioned.exclusion_date
        , exclusions_unioned.exclusion_reason
        , case
            when exclusion_reason = 'pregnancy' then 1
            else 0
          end as is_pregnant
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.age
    from exclusions_unioned
    inner join denominator
        on exclusions_unioned.patient_id = denominator.patient_id

)

, exclusions_filtered as (

    select
          patient_id
        , age
        , exclusion_date
        , exclusion_reason
    from excluded_patients
    where is_pregnant = 1
        and exclusion_date between performance_period_begin and performance_period_end
    
    union all

    select
          patient_id
        , age
        , exclusion_date
        , exclusion_reason
    from excluded_patients
    where is_pregnant = 0
        and exclusion_date between performance_period_begin and performance_period_end
          or (exclusion_date between 

    dateadd(
        year,
        -1,
        performance_period_begin
        )


            and performance_period_end)

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , age
    , 'measure specific exclusion for observation procedure' as exclusion_type
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from
    exclusions_filtered