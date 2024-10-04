
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_shared_exclusions_hospice_palliative
         as
        (

/*
DENOMINATOR EXCLUSIONS:
Patient was provided hospice services any time during the measurement period: G9687
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
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.condition
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
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
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
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), exclusion_codes as (

    select
          code
        , case code_system
            when 'SNOMEDCT' then 'snomed-ct'
            when 'ICD9CM' then 'icd-9-cm'
            when 'ICD10CM' then 'icd-10-cm'
            when 'CPT' then 'hcpcs'
            when 'ICD10PCS' then 'icd-10-pcs'
          else lower(code_system) end as code_system
        , concept_name
    From tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
            'hospice encounter'
          , 'hospice care ambulatory'
          , 'hospice diagnosis'
          , 'palliative care encounter'
          , 'palliative care diagnosis'
          , 'palliative care intervention'
    )
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

, medical_claim as (

    select
          patient_id
        , claim_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from __dbt__cte__quality_measures__stg_medical_claim

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
        , coalesce (
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
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

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

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name as concept_name
    from observations
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system

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

, patients_with_exclusions as(
    
    select patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select patient_id
        , observation_date as exclusion_date
        , concept_name as exclusion_reason
    from observation_exclusions

    union all

    select patient_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , 'hospice_palliative' as exclusion_type
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from patients_with_exclusions
        );
      
  