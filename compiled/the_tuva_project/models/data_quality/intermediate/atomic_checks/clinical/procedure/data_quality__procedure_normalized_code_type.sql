


SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_CODE_TYPE' as field_name
    , case when term.code_type is not null then 'valid'
           when m.normalized_code_type is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.normalized_code_type is not null and term.code_type is null
           then 'Normalized code type does not join to Terminology code_type table'
           else null end as invalid_reason
    , cast(normalized_code_type as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.procedure m
left join tuva_synthetic.reference_data.code_type term on m.normalized_code_type = term.code_type