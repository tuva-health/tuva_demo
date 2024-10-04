

SELECT
    m.data_source
    ,coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    ,'ENCOUNTER' AS table_name
    ,'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    ,'LENGTH_OF_STAY' AS field_name
    ,case when m.length_of_stay is not null then 'valid' else 'null' end as bucket_name
    ,cast(null as TEXT) as invalid_reason
    ,cast(length_of_stay as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m