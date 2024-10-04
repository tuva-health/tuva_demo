

with  __dbt__cte__hcc_suspecting__stg_core__medication as (

select
      patient_id
    , dispensing_date
    , source_code
    , source_code_type
    , ndc_code
    , rxnorm_code
    , data_source
from tuva_synthetic.core.medication


),  __dbt__cte__hcc_suspecting__stg_core__pharmacy_claim as (


    select
          cast(null as TEXT ) as patient_id
        , try_cast( null as date ) as dispensing_date
        , cast(null as TEXT ) as ndc_code
        , try_cast( null as date ) as paid_date
        , cast(null as TEXT ) as data_source
    limit 0
), medications as (

    select
          patient_id
        , dispensing_date
        , source_code
        , source_code_type
        , ndc_code
        , rxnorm_code
        , data_source
    from __dbt__cte__hcc_suspecting__stg_core__medication

)

, pharmacy_claims as (

    select
          patient_id
        , coalesce(dispensing_date, paid_date) as dispensing_date
        , ndc_code as drug_code
        , 'ndc' as code_system
        , data_source
    from __dbt__cte__hcc_suspecting__stg_core__pharmacy_claim

)

, ndc_medications as (

    select
          patient_id
        , dispensing_date
        , ndc_code as drug_code
        , 'ndc' as code_system
        , data_source
    from medications
    where ndc_code is not null

    union all

    select
          patient_id
        , dispensing_date
        , source_code as drug_code
        , 'ndc' as code_system
        , data_source
    from medications
    where lower(source_code_type) = 'ndc'

)

, rxnorm_medications as (

    select
          patient_id
        , dispensing_date
        , rxnorm_code as drug_code
        , 'rxnorm' as code_system
        , data_source
    from medications
    where rxnorm_code is not null

    union all

    select
          patient_id
        , dispensing_date
        , source_code as drug_code
        , 'rxnorm' as code_system
        , data_source
    from medications
    where lower(source_code_type) = 'rxnorm'

)

, unioned as (

    select * from pharmacy_claims
    union all
    select * from ndc_medications
    union all
    select * from rxnorm_medications

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(dispensing_date as date) as dispensing_date
        , cast(drug_code as TEXT) as drug_code
        , cast(code_system as TEXT) as code_system
        , cast(data_source as TEXT) as data_source
    from unioned

)

select
      patient_id
    , dispensing_date
    , drug_code
    , code_system
    , data_source
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types