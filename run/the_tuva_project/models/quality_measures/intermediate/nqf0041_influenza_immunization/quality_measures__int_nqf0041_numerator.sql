
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_nqf0041_numerator
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
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from tuva_synthetic.quality_measures._int_nqf0041_denominator

)

, influenza_vaccination_code as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
          'influenza vaccination'
        , 'influenza vaccine'
        , 'influenza virus laiv immunization'
        , 'influenza virus laiv procedure'
    )

)

, procedure_vaccination as (

    select
        patient_id
      , procedure_date
    from __dbt__cte__quality_measures__stg_core__procedure as procedures
    inner join influenza_vaccination_code
        on coalesce(procedures.normalized_code, procedures.source_code) = influenza_vaccination_code.code
            and coalesce(procedures.normalized_code_type, procedures.source_code_type) = influenza_vaccination_code.code_system

)

, claims_vaccination as (
    
    select 
          patient_id
        , coalesce(claim_start_date,claim_end_date) as min_date
        , coalesce(claim_end_date,claim_start_date) as max_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join influenza_vaccination_code
        on medical_claim.hcpcs_code = influenza_vaccination_code.code

)

, qualifying_procedures as (

    select
          procedure_vaccination.patient_id
        , procedure_vaccination.procedure_date as evidence_date
    from procedure_vaccination
    inner join tuva_synthetic.quality_measures._int_nqf0041__performance_period pp
        on procedure_date between 
            pp.lookback_period_august and
                pp.performance_period_end

)

, qualifying_claims as (

    select 
          claims_vaccination.patient_id
        , claims_vaccination.max_date as evidence_date
    from claims_vaccination
    inner join tuva_synthetic.quality_measures._int_nqf0041__performance_period pp
        on max_date between
            pp.lookback_period_august and
                pp.performance_period_end

)

, qualified_patients as (

    select
          patient_id
        , evidence_date
    from qualifying_procedures

    union all

    select
          patient_id
        , evidence_date
    from qualifying_claims

)

, combined_qualifying_patients as (

    select
          qualified_patients.patient_id
        , qualified_patients.evidence_date
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , 1 as numerator_flag
    from qualified_patients
    inner join denominator
        on qualified_patients.patient_id = denominator.patient_id

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
        , cast(null as TEXT) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from combined_qualifying_patients

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
      
  