


    select
      cast(null as TEXT ) as patient_id
    , try_cast( null as date ) as dispensing_date
    , cast(null as TEXT ) as ndc_code
    , try_cast( null as date ) as paid_date
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0