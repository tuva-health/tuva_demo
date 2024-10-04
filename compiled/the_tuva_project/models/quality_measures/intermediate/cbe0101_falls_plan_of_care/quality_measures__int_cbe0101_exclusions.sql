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
    from tuva_synthetic.quality_measures._int_cbe0101_denominator

)

, exclusion_code as (

    select
        code
      , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where code = '0518F'
    -- further 1P modifier are only excluded
)

, hospice_palliative as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from tuva_synthetic.quality_measures._int_shared_exclusions_hospice_palliative
    where exclusion_date between (

  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cbe0101__performance_period

) and (

  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cbe0101__performance_period

)

)

, valid_hospice as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from hospice_palliative
    where lower(exclusion_reason) in (
            'hospice encounter'
          , 'hospice care ambulatory'
          , 'hospice diagnosis'
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
        , modifier_1
        , modifier_2
        , modifier_3
        , modifier_4
        , modifier_5
    from __dbt__cte__quality_measures__stg_core__procedure

)

, exclusion_procedures as (

    select
          patient_id
        , procedure_date as exclusion_date
        , 'Limited Mobility' as exclusion_reason
    from procedures
    inner join exclusion_code
        on procedures.code = exclusion_code.code
            and procedures.code_type = exclusion_code.code_system
    where '1P' in (modifier_1, modifier_2, modifier_3, modifier_4, modifier_5)
            
)

, exclusion_claims as (

    select
          patient_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , 'Limited mobility' as exclusion_reason
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join exclusion_code
        on medical_claim.hcpcs_code = exclusion_code.code
            and lower(exclusion_code.code_system) = 'hcpcs'
    where '1P' in (hcpcs_modifier_1, hcpcs_modifier_2, hcpcs_modifier_3, hcpcs_modifier_4, hcpcs_modifier_5)

)

, exclusion_patients as (

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from valid_hospice

    union all

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from exclusion_procedures

    union all

    select
        patient_id
      , exclusion_date
      , exclusion_reason
    from exclusion_claims

)

, combined_exclusions as (

    select
        exclusion_patients.patient_id
      , exclusion_patients.exclusion_date
      , exclusion_patients.exclusion_reason
    from exclusion_patients
    inner join denominator
      on exclusion_patients.patient_id = denominator.patient_id

)

, add_data_types as (

    select
        distinct
          cast(patient_id as TEXT) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from combined_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types