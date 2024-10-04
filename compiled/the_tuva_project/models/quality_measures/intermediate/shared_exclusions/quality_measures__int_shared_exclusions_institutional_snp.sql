

/*
    patients in institutional special needs plans (snp)
    or residing in long term care

    while referencing this model, patients greater or equal than 66 years of age should be taken

    filtering out age from this model has been stripped out as different measures calculate age varingly

    future enhancement: group claims into encounters
*/

with  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.patient
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
), patients as (

    select
          patient_id
    from __dbt__cte__quality_measures__stg_core__patient

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from __dbt__cte__quality_measures__stg_medical_claim

)

, exclusions as (

    select
          patients.patient_id
        , coalesce(
              medical_claim.claim_start_date
            , medical_claim.claim_end_date
          ) as exclusion_date
        , 'institutional or long term care' as exclusion_reason
    from patients
         inner join medical_claim
         on patients.patient_id = medical_claim.patient_id
    where place_of_service_code in ('32', '33', '34', '54', '56')
    and datediff(
        day,
        medical_claim.claim_start_date,
        medical_claim.claim_end_date
        ) >= 90

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , 'institutional_snp' as exclusion_type
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from exclusions