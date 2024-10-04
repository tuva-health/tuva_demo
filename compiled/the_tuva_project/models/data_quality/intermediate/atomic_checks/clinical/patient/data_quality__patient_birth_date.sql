


SELECT
      m.data_source
    
        , cast(coalesce(convert_timezone('UTC', current_timestamp()), cast('1900-01-01' as date)) as date) as source_date
    
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'BIRTH_DATE' AS field_name
    , case
        when m.birth_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'invalid'
        when m.birth_date <= cast('1901-01-01' as date) then 'invalid'
        when m.birth_date > m.death_date then 'invalid'
        when m.birth_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.birth_date > cast(substring('2024-10-04 19:11:18.274664+00:00',1,10) as date) then 'future'
        when m.birth_date <= cast('1901-01-01' as date) then 'too old'
        when m.birth_date > m.death_date then 'Birth date after death date'
        else null
    end as invalid_reason
    , cast(birth_date as TEXT) as field_value
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.input_layer.patient m