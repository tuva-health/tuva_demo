

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'DISCHARGE_DISPOSITION_CODE' AS field_name
    , case when term.discharge_disposition_code is not null then 'valid'
           when m.discharge_disposition_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.discharge_disposition_code is not null and term.discharge_disposition_code is null
           then 'Discharge Disposition Code does not join to Terminology discharge_disposition table'
           else null end as invalid_reason
    , cast(m.discharge_disposition_code as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.terminology.discharge_disposition term on m.discharge_disposition_code = term.discharge_disposition_code