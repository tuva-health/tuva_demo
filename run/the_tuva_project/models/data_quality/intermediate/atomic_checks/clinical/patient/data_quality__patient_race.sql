
  
    

        create or replace transient table tuva_synthetic.data_quality.patient_race
         as
        (


SELECT
      m.data_source
    
        , cast(coalesce(convert_timezone('UTC', current_timestamp()), cast('1900-01-01' as date)) as date) as source_date
    
    , 'PATIENT' AS table_name
    , 'Patient ID' as drill_down_key
    , coalesce(patient_id, 'NULL') AS drill_down_value
    , 'RACE' as field_name
    , case when term.description is not null then 'valid'
           when m.race is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.race is not null and term.description is null
           then 'Race description does not join to Terminology race table'
           else null end as invalid_reason
    , cast(race as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.patient m
left join tuva_synthetic.terminology.race term on m.race = term.description
        );
      
  