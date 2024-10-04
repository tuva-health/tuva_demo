


SELECT
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' AS table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') AS drill_down_value
    , 'PRESCRIBING_DATE' as field_name
    , case
        when m.prescribing_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'invalid'
        when m.prescribing_date <= cast('1901-01-01' as date) then 'invalid'
        when m.prescribing_date > m.dispensing_date then 'invalid'
        when m.prescribing_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.prescribing_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'future'
        when m.prescribing_date <= cast('1901-01-01' as date) then 'too old'
        when m.prescribing_date > m.dispensing_date then 'Prescribing date after dispensing date'
        else null
    end as invalid_reason
    , cast(prescribing_date as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.medication m