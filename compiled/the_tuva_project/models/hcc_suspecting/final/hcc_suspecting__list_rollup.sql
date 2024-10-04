

with list as (

    select
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , row_number() over (
            partition by
                  patient_id
                , hcc_code
            order by suspect_date desc
          ) as row_num
    from tuva_synthetic.hcc_suspecting.list

)

, list_dedupe as (

    select
          patient_id
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date as latest_suspect_date
    from list
    where row_num = 1

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(hcc_code as TEXT) as hcc_code
        , cast(hcc_description as TEXT) as hcc_description
        , cast(reason as TEXT) as reason
        , cast(contributing_factor as TEXT) as contributing_factor
        , cast(latest_suspect_date as date) as latest_suspect_date
    from list_dedupe

)

select
      patient_id
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , latest_suspect_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types