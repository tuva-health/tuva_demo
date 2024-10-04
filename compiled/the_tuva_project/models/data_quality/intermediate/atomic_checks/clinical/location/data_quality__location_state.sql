


SELECT
      m.data_source
    
        , cast(coalesce(convert_timezone('UTC', current_timestamp()), cast('1900-01-01' as date)) as date) as source_date
    
    , 'LOCATION' AS table_name
    , 'Location ID' as drill_down_key
    , coalesce(location_id, 'NULL') AS drill_down_value
    , 'STATE' as field_name
    , case when m.state is not null then 'valid' else 'null' end as bucket_name
    , cast(null as TEXT) as invalid_reason
    , cast(state as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.location m