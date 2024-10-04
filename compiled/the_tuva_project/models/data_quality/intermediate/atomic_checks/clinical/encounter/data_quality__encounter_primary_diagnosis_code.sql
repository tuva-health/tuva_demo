

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'PRIMARY_DIAGNOSIS_CODE' AS field_name
    , case when term.icd_10_cm is not null then 'valid'
          when m.primary_diagnosis_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.primary_diagnosis_code is not null and term.icd_10_cm is null
          then 'Primary diagnosis code does not join to Terminology icd_10_cm table'
    else null end as invalid_reason
    , cast(primary_diagnosis_code as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.terminology.icd_10_cm term on m.primary_diagnosis_code = term.icd_10_cm