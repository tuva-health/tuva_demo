
  
    

        create or replace transient table tuva_synthetic.data_quality.practitioner_first_name
         as
        (


SELECT
      m.data_source
    
        , cast(coalesce(convert_timezone('UTC', current_timestamp()), cast('1900-01-01' as date)) as date) as source_date
    
    , 'PRACTITIONER' AS table_name
    , 'Practitioner ID' as drill_down_key
    , coalesce(practitioner_id, 'NULL') AS drill_down_value
    , 'FIRST_NAME' as field_name
    , case when m.first_name is not null then 'valid' else 'null' end as bucket_name
    , cast(null as TEXT) as invalid_reason
    , cast(first_name as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.practitioner m
        );
      
  