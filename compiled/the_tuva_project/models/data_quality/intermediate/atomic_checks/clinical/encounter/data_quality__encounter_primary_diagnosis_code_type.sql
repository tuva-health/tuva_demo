

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'PRIMARY_DIAGNOSIS_CODE_TYPE' AS field_name
    , case when term.code_type is not null then 'valid'
          when m.primary_diagnosis_code_type is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.primary_diagnosis_code_type is not null and term.code_type is null
          then 'Primary Diagnosis Code Type does not join to Terminology code_type table'
          else null end as invalid_reason
    , cast(primary_diagnosis_code_type as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.reference_data.code_type term on m.primary_diagnosis_code_type = term.code_type