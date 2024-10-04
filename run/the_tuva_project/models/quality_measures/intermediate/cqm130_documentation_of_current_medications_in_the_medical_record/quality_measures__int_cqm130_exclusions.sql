
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_cqm130_exclusions
         as
        (with  __dbt__cte__quality_measures__stg_core__condition as (

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
),  __dbt__cte__quality_measures__stg_medical_claim as (



select
         cast(null as TEXT ) as patient_id
        , cast(null as TEXT ) as claim_id
        , try_cast( null as date ) as claim_start_date
        , try_cast( null as date ) as claim_end_date
        , cast(null as TEXT ) as place_of_service_code
        , cast(null as TEXT ) as hcpcs_code
        , cast(null as TEXT ) as hcpcs_modifier_1
        , cast(null as TEXT ) as hcpcs_modifier_2
        , cast(null as TEXT ) as hcpcs_modifier_3
        , cast(null as TEXT ) as hcpcs_modifier_4
        , cast(null as TEXT ) as hcpcs_modifier_5
        , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0
), exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
            'medical reason'
    )

)

, conditions as (

    select
          patient_id
        , recorded_date
        , claim_id
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
    where recorded_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
)

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
    where procedure_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
)

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
    from __dbt__cte__quality_measures__stg_medical_claim
    where coalesce(claim_end_date, claim_start_date) between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm130__performance_period
)

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name as concept_name
    from conditions
    inner join exclusion_codes
      on conditions.code = exclusion_codes.code
        and conditions.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name as concept_name
    from procedures
    inner join exclusion_codes
      on procedures.code = exclusion_codes.code
        and procedures.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , coalesce(medical_claim.claim_end_date, medical_claim.claim_start_date) as exclusion_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
    inner join exclusion_codes
      on medical_claim.hcpcs_code = exclusion_codes.code
        and exclusion_codes.code_system = 'hcpcs'

)

, patients_with_exclusions as (
    
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

    union all

    select
          patient_id
        , exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

)

, valid_exclusions as (

  select 
        patients_with_exclusions.patient_id
      , patients_with_exclusions.exclusion_date
      , patients_with_exclusions.exclusion_reason  
  from patients_with_exclusions
  inner join tuva_synthetic.quality_measures._int_cqm130_denominator as denominator
      on patients_with_exclusions.patient_id = denominator.patient_id

)

, add_data_types as (

    select
        distinct
          cast(patient_id as TEXT) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  