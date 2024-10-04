
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_nqf2372_numerator
         as
        (

/*
    Eligible population from the denominator model before exclusions
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
), denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , performance_period_lookback
        , measure_id
        , measure_name
        , measure_version
    from tuva_synthetic.quality_measures._int_nqf2372_denominator

)

, mammography_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where concept_name = 'Mammography'

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

, qualifying_claims as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
    from medical_claim
         inner join mammography_codes
            on medical_claim.hcpcs_code = mammography_codes.code
    where mammography_codes.code_system = 'hcpcs'

)

, qualifying_observations as (

    select
          observations.patient_id
        , observations.observation_date
    from observations
         inner join mammography_codes
             on observations.code = mammography_codes.code
             and observations.code_type = mammography_codes.code_system
)

, qualifying_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
    from procedures
         inner join mammography_codes
             on procedures.code = mammography_codes.code
             and procedures.code_type = mammography_codes.code_system

)

/*
    Check if patients in the eligible population have had a screening,
    diagnostic, film, digital or digital breast tomosynthesis (3D)
    mammography results documented and reviewed.
*/

, patients_with_mammograms as (

    select
          denominator.patient_id
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.performance_period_lookback
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , case
            when qualifying_claims.claim_start_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then qualifying_claims.claim_start_date
            when qualifying_claims.claim_end_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then qualifying_claims.claim_end_date
            when qualifying_observations.observation_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then qualifying_observations.observation_date
            when qualifying_procedures.procedure_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then qualifying_procedures.procedure_date
            else null
          end as evidence_date
        , case
            when qualifying_claims.claim_start_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then 1
            when qualifying_claims.claim_end_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then 1
            when qualifying_observations.observation_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then 1
            when qualifying_procedures.procedure_date
                between denominator.performance_period_lookback
                and denominator.performance_period_end
                then 1
            else 0
          end as numerator_flag
    from denominator
         left join qualifying_claims
            on denominator.patient_id = qualifying_claims.patient_id
        left join qualifying_observations
            on denominator.patient_id = qualifying_observations.patient_id
        left join qualifying_procedures
            on denominator.patient_id = qualifying_procedures.patient_id

)

, add_data_types as (

     select distinct
          cast(patient_id as TEXT) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(numerator_flag as integer) as numerator_flag
    from patients_with_mammograms

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , numerator_flag
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  