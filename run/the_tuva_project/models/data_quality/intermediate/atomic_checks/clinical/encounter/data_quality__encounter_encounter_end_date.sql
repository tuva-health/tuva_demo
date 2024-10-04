
  
    

        create or replace transient table tuva_synthetic.data_quality.encounter_encounter_end_date
         as
        (

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'ENCOUNTER_END_DATE' AS field_name
    , case
        when m.encounter_end_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'invalid'
        when m.encounter_end_date <= cast('1901-01-01' as date) then 'invalid'
        when m.encounter_end_date < m.encounter_start_date then 'invalid'
        when m.encounter_end_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.encounter_end_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'future'
        when m.encounter_end_date <= cast('1901-01-01' as date) then 'too old'
        when m.encounter_end_date < m.encounter_start_date then 'Encounter end date before encounter start date'
        else null
    end as invalid_reason
    , cast(encounter_end_date as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
        );
      
  