


SELECT
      m.data_source
    , coalesce(m.observation_date,cast('1900-01-01' as date)) as source_date
    , 'OBSERVATION' AS table_name
    , 'Observation ID' as drill_down_key
    , coalesce(observation_id, 'NULL') AS drill_down_value
    , 'OBSERVATION_DATE' as field_name
    , case
        when m.observation_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'invalid'
        when m.observation_date <= cast('1901-01-01' as date) then 'invalid'
        when m.observation_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.observation_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'future'
        when m.observation_date <= cast('1901-01-01' as date) then 'too old'
        else null
    end as invalid_reason
    , cast(observation_date as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.observation m