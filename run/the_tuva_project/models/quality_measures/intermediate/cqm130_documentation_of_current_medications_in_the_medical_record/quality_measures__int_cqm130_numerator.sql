
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_cqm130_numerator
         as
        (

with  __dbt__cte__quality_measures__stg_core__procedure as (

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
), denominator as (

    select
          patient_id
        , procedure_encounter_date
        , claims_encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from tuva_synthetic.quality_measures._int_cqm130_denominator

)

, medication_code as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
          'eligible clinician attests to documenting current medications'
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

)

, documenting_meds_procedures as (

    select
          patient_id
        , procedure_date
    from procedures
    inner join medication_code
      on procedures.code = medication_code.code
        and procedures.code_type = medication_code.code_system

)

, documenting_meds_claims as (

    select
          patient_id
        , coalesce(claim_end_date,claim_start_date) as encounter_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join medication_code
        on medical_claim.hcpcs_code = medication_code.code
          and medication_code.code_system = 'hcpcs'

)

, qualifying_procedure as (

    select 
          documenting_meds_procedures.patient_id
        , documenting_meds_procedures.procedure_date as encounter_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
    from documenting_meds_procedures
    inner join denominator
      on documenting_meds_procedures.patient_id = denominator.patient_id
        and documenting_meds_procedures.procedure_date = denominator.procedure_encounter_date

)

, qualifying_claims as (
    
    select 
          documenting_meds_claims.patient_id
        , documenting_meds_claims.encounter_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
    from documenting_meds_claims
    inner join denominator
      on documenting_meds_claims.patient_id = denominator.patient_id
        and documenting_meds_claims.encounter_date = denominator.claims_encounter_date

)

, qualifying_cares as (

    select
          patient_id
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , cast(1 as integer) as numerator_flag
    from qualifying_procedure

    union all

    select
          patient_id
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , cast(1 as integer) as numerator_flag
    from qualifying_claims

)

, add_data_types as (

     select distinct
          cast(patient_id as TEXT) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(encounter_date as date) as evidence_date
        , cast(null as TEXT) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
      from qualifying_cares

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  