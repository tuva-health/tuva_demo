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
),  __dbt__cte__quality_measures__stg_core__medication as (


select
      patient_id
    , encounter_id
    , prescribing_date   
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.medication


),  __dbt__cte__quality_measures__stg_pharmacy_claim as (



    select
      cast(null as TEXT ) as patient_id
    , try_cast( null as date ) as dispensing_date
    , cast(null as TEXT ) as ndc_code
    , try_cast( null as date ) as paid_date
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0
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
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
            'rhabdomyolysis'
          , 'breastfeeding'
          , 'liver disease'
          , 'hepatitis a'
          , 'hepatitis b'
          , 'documentation of medical reason for no statin therapy'
          , 'statin allergen'
          , 'end stage renal disease'
          , 'statin associated muscle symptoms'
          , 'medical reason'
    )

)

, valid_hospice_palliative as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from tuva_synthetic.quality_measures._int_shared_exclusions_hospice_palliative
  where exclusion_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

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
    where recorded_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

)

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
    where coalesce(claim_end_date, claim_start_date) between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

)

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
    where observation_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

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
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm438__performance_period

)

)

, medications as (
    
    select
        patient_id
      , coalesce(prescribing_date, dispensing_date) as exclusion_date
      , source_code
      , source_code_type
    from __dbt__cte__quality_measures__stg_core__medication

)

, pharmacy_claims as (

    select
        patient_id
      , dispensing_date
      , ndc_code
    from __dbt__cte__quality_measures__stg_pharmacy_claim 

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

, medication_exclusions as (

    select
          medications.patient_id
        , medications.exclusion_date
        , exclusion_codes.concept_name as concept_name
    from medications
    inner join exclusion_codes
        on medications.source_code = exclusion_codes.code
          and medications.source_code_type = exclusion_codes.code_system

)

, pharmacy_claims_exclusions as (

    select
          pharmacy_claims.patient_id
        , pharmacy_claims.dispensing_date
        , exclusion_codes.concept_name as concept_name
    from pharmacy_claims
    inner join exclusion_codes
        on pharmacy_claims.ndc_code = exclusion_codes.code
        and lower(exclusion_codes.code_system) = 'ndc'

)

, patients_with_exclusions as(
    
    select 
          patient_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select 
          patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select 
          patient_id
        , observation_date as exclusion_date
        , concept_name as exclusion_reason
    from observation_exclusions

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
    from medication_exclusions

    union all

    select
          patient_id
        , dispensing_date as exclusion_date
        , concept_name as exclusion_reason
    from pharmacy_claims_exclusions

    union all

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from valid_hospice_palliative

)

, valid_exclusions as (

  select 
        patients_with_exclusions.patient_id
      , patients_with_exclusions.exclusion_date
      , patients_with_exclusions.exclusion_reason  
  from patients_with_exclusions
  inner join tuva_synthetic.quality_measures._int_cqm438_denominator as denominator
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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types