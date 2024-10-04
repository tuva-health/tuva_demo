
  
    

        create or replace transient table tuva_synthetic.data_quality.location_npi
         as
        (


SELECT
      m.data_source
    
        , cast(coalesce(convert_timezone('UTC', current_timestamp()), cast('1900-01-01' as date)) as date) as source_date
    
    , 'LOCATION' AS table_name
    , 'Location ID' as drill_down_key
    , coalesce(location_id, 'NULL') AS drill_down_value
    , 'NPI' as field_name
    , case when term.npi is not null then 'valid'
          when m.npi is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.npi is not null and term.npi is null
          then 'NPI does not join to Terminology provider table'
    else null end as invalid_reason
    , cast(m.npi as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.location m
left join tuva_synthetic.terminology.provider term on m.npi = term.npi
        );
      
  