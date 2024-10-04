

SELECT
    m.data_source
    ,coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    ,'CONDITION' AS table_name
    ,'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') AS drill_down_value
    ,'ONSET_DATE' AS field_name
    ,case
        when m.onset_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'invalid'
        when m.onset_date <= cast('1901-01-01' as date) then 'invalid'
        when m.onset_date > m.resolved_date then 'invalid'
        when m.onset_date is null then 'null'
        else 'valid'
    end as bucket_name
    ,case
        when m.onset_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'future'
        when m.onset_date <= cast('1901-01-01' as date) then 'too old'
        WHEN m.onset_date < m.resolved_date THEN 'Onset date after resolved date'
        else null
    end as invalid_reason
    ,cast(onset_date as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.condition m