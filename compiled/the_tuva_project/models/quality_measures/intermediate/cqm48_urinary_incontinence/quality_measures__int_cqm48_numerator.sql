

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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
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
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from tuva_synthetic.quality_measures._int_cqm48_denominator

)

, involuntary_urination_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
            'presence or absence of urinary incontinence assessed'
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

, qualifying_procedures as (

    select
          patient_id
        , procedure_date as evidence_date
    from procedures
    inner join involuntary_urination_codes
        on procedures.code = involuntary_urination_codes.code
            and procedures.code_type = involuntary_urination_codes.code_system
            
)

, qualifying_claims as (

    select
          patient_id
        , coalesce(claim_end_date, claim_start_date) as evidence_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join involuntary_urination_codes
        on medical_claim.hcpcs_code = involuntary_urination_codes.code
            and lower(involuntary_urination_codes.code_system) = 'hcpcs'

)

, qualifying_cares as (

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
          qualifying_cares.patient_id
        , qualifying_cares.evidence_date
        , cast(null as TEXT) as evidence_value
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , 1 as numerator_flag
    from qualifying_cares
    inner join denominator
        on qualifying_cares.patient_id = denominator.patient_id
    where evidence_date between
        

    dateadd(
        year,
        -1,
        denominator.encounter_date
        )


        and 
        denominator.encounter_date -- within last 12 months of visit

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
        , cast(evidence_value as TEXT) as evidence_value
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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types