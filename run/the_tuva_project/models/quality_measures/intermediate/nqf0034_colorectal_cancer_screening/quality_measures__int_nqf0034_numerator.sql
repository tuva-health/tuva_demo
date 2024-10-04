
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_nqf0034_numerator
         as
        (


/*
Patients with one or more screenings for colorectal cancer. Appropriate screenings are defined by any one of the
following criteria:
- Fecal occult blood test (FOBT) during the measurement period
- Flexible sigmoidoscopy during the measurement period or the four years prior to the measurement period
- Colonoscopy during the measurement period or the nine years prior to the measurement period
- Computed tomography (CT) colonography during the measurement period or the four years prior to the
measurement period.
- Fecal immunochemical DNA test (FIT-DNA) during the measurement period or the two years prior to the
measurement period.

*/

with  __dbt__cte__quality_measures__stg_medical_claim as (



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


), denominator as (

    select
          patient_id
    from tuva_synthetic.quality_measures._int_nqf0034_denominator

)

, screening_codes as (

    select
        code
      , case code_system
          when 'SNOMEDCT' then 'snomed-ct'
          when 'ICD9CM' then 'icd-9-cm'
          when 'ICD10CM' then 'icd-10-cm'
          when 'CPT' then 'hcpcs'
          when 'ICD10PCS' then 'icd-10-pcs'
        else lower(code_system) 
        end as code_system
      , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
          'fecal occult blood test (fobt)' -- mp
        , 'flexible sigmoidoscopy' --mp+4
        , 'colonoscopy' -- mp+9
        , 'ct colonography' -- mp+4
        , 'sdna fit test' -- mp+2
    )

)

, screening_periods  as (

    select 
          code
        , code_system
        , concept_name
        , case concept_name
            when 'Fecal Occult Blood Test (FOBT)' then pp.performance_period_begin --mp
            when 'Flexible Sigmoidoscopy' then pp.performance_period_begin_4yp --mp+4
            when 'Colonoscopy' then pp.performance_period_begin_9yp -- mp+9
            when 'CT Colonography' then pp.performance_period_begin_4yp -- mp+4
            when 'sDNA FIT Test' then pp.performance_period_begin_2yp -- mp+2
          else pp.performance_period_begin 
          end as effective_performance_period_begin
        , performance_period_end
    from screening_codes
    inner join tuva_synthetic.quality_measures._int_nqf0034__performance_period pp
        on 1 = 1
    
)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
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
        , coalesce(
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

, labs as (

    select  
      patient_id
    , result_date
    , collection_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    from __dbt__cte__quality_measures__stg_core__lab_result

)

, qualifying_claims as (

    select
          medical_claim.patient_id
        , coalesce( medical_claim.claim_start_date, medical_claim.claim_end_date) as claim_date
    , screening_codes.concept_name
    from medical_claim
    inner join screening_periods
        on medical_claim.claim_start_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
        or medical_claim.claim_end_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
    inner join screening_codes
            on medical_claim.hcpcs_code = screening_codes.code
    where screening_codes.code_system in ('hcpcs', 'cpt' )

)

, qualifying_observations as (

    select
          observations.patient_id
        , observations.observation_date
        , screening_codes.concept_name
    from observations
    inner join screening_periods
        on observations.observation_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
     inner join screening_codes
         on observations.code = screening_codes.code
         and observations.code_type = screening_codes.code_system
)

, qualifying_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , screening_codes.concept_name
    from procedures
    inner join screening_periods
        on procedures.procedure_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
     inner join screening_codes
         on procedures.code = screening_codes.code
         and procedures.code_type = screening_codes.code_system

)

, normalized_code_labs as (
  select
      patient_id
    , coalesce(collection_date, result_date) as lab_date
    , screening_codes.concept_name
  from labs
  inner join screening_periods
    on coalesce(labs.collection_date, labs.result_date) >= screening_periods.effective_performance_period_begin
    and coalesce(labs.collection_date, labs.result_date) <= screening_periods.performance_period_end
  inner join screening_codes
    on labs.normalized_code = screening_codes.code
    and labs.normalized_code_type = screening_codes.code_system
)

, source_code_labs as (

  select
      patient_id
    , coalesce(collection_date, result_date) as lab_date
    , screening_codes.concept_name
  from labs
  inner join screening_periods
    on coalesce(labs.collection_date, labs.result_date) >= screening_periods.effective_performance_period_begin
    and coalesce(labs.collection_date, labs.result_date) <= screening_periods.performance_period_end
  inner join screening_codes
    on labs.source_code = screening_codes.code
    and labs.source_code_type = screening_codes.code_system

)

, qualifying_labs_union as (

  select 
        patient_id
      , lab_date
      , concept_name
  from normalized_code_labs

  union all

  select 
        patient_id
      , lab_date
      , concept_name
  from source_code_labs

)

, qualifying_labs as (

  select distinct
        patient_id
      , lab_date
      , concept_name
  from qualifying_labs_union

)

, qualifying_events as (

    select
          patient_id
        , claim_date as evidence_date
        , concept_name as evidence
    from qualifying_claims

    union all

    select
          patient_id
        , observation_date as evidence_date
        , concept_name as evidence
    from qualifying_observations

    union all

    select
          patient_id
        , procedure_date as evidence_date
        , concept_name as evidence
    from qualifying_procedures

    union all

    select
          patient_id
        , lab_date as evidence_date
        , concept_name as evidence
    from qualifying_labs

)

select
      cast( qualifying_events.patient_id as TEXT) as patient_id
    , cast( evidence_date as date) as evidence_date
    , cast( evidence as TEXT) as evidence
from qualifying_events
inner join denominator
    on qualifying_events.patient_id = denominator.patient_id
        );
      
  