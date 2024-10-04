
  
    

        create or replace transient table tuva_synthetic.data_quality.observation_normalized_reference_range_low
         as
        (


SELECT
      m.data_source
    , coalesce(m.observation_date,cast('1900-01-01' as date)) as source_date
    , 'OBSERVATION' AS table_name
    , 'Observation ID' as drill_down_key
    , coalesce(observation_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_REFERENCE_RANGE_LOW' AS field_name
    , case when m.normalized_reference_range_low is not null then 'valid' else 'null' end as bucket_name
    , cast(null as TEXT) as invalid_reason
    , cast(normalized_reference_range_low as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.observation m
        );
      
  