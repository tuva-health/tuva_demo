

/*
    Women who had a bilateral mastectomy or who have a history of a bilateral
    mastectomy or for whom there is evidence of a right and a left
    unilateral mastectomy
*/

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
),  __dbt__cte__quality_measures__stg_core__observation as (


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
        , performance_period_begin
        , performance_period_end
    from tuva_synthetic.quality_measures._int_nqf2372_denominator

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where concept_name in (
          'Bilateral Mastectomy'
        , 'History of bilateral mastectomy'
        , 'Status Post Left Mastectomy'
        , 'Status Post Right Mastectomy'
        , 'Unilateral Mastectomy Left'
        , 'Unilateral Mastectomy Right'
        , 'Unilateral Mastectomy, Unspecified Laterality'
    )

)

, conditions as (

    select
          patient_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__condition

)

, observations as (

    select
          patient_id
        , observation_date
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
    from __dbt__cte__quality_measures__stg_core__observation

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

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
             on conditions.code = exclusion_codes.code
             and conditions.code_type = exclusion_codes.code_system

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
         inner join exclusion_codes
             on observations.code = exclusion_codes.code
             and observations.code_type = exclusion_codes.code_system

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

, all_mastectomy as (

    select
          denominator.patient_id
        , condition_exclusions.recorded_date as exclusion_date
        , condition_exclusions.concept_name as exclusion_reason
    from denominator
         inner join condition_exclusions
            on denominator.patient_id = condition_exclusions.patient_id

    union all

    select
          denominator.patient_id
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
    from denominator
         inner join observation_exclusions
            on denominator.patient_id = observation_exclusions.patient_id

    union all

    select
          denominator.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
            on denominator.patient_id = procedure_exclusions.patient_id

)

/*
    Women who had a bilateral mastectomy or who have a history of a bilateral
    mastectomy
*/
, bilateral_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Bilateral Mastectomy'
        , 'History of bilateral mastectomy'
    )

)

, right_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Status Post Right Mastectomy'
        , 'Unilateral Mastectomy Right'
    )

)

, left_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Status Post Left Mastectomy'
        , 'Unilateral Mastectomy Left'
    )

)

, unspecified_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
        'Unilateral Mastectomy, Unspecified Laterality'
    )

)

/*
    Women for whom there is evidence of a right AND a left unilateral mastectomy
    or unspecific mastectomies on different dates
*/
, unilateral_mastectomy as (

    select
          right_mastectomy.patient_id
        , right_mastectomy.exclusion_date
        , right_mastectomy.exclusion_reason
    from right_mastectomy
         inner join left_mastectomy
            on right_mastectomy.patient_id = left_mastectomy.patient_id

    union all

    select
          unspecified_mastectomy.patient_id
        , unspecified_mastectomy.exclusion_date
        , unspecified_mastectomy.exclusion_reason
    from unspecified_mastectomy
         inner join unspecified_mastectomy as self_join
            on unspecified_mastectomy.patient_id = self_join.patient_id
            and unspecified_mastectomy.exclusion_date <> self_join.exclusion_date

)

, unioned as (

    select * from bilateral_mastectomy
    union all
    select * from unilateral_mastectomy
)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from unioned