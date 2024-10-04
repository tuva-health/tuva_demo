

SELECT
      m.data_source
    , coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    , 'CONDITION' AS table_name
    , 'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_CODE' AS field_name
    , case when term.icd_10_cm is not null then 'valid'
          when m.normalized_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.icd_10_cm is null
           then 'Normalized code does not join to Terminology icd_10_cm table'
           else null end as invalid_reason
    , cast(normalized_code as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.condition m
left join tuva_synthetic.terminology.icd_10_cm term on m.normalized_code = term.icd_10_cm