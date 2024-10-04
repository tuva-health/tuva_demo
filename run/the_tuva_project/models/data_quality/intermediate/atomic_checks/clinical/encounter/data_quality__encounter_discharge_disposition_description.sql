
  
    

        create or replace transient table tuva_synthetic.data_quality.encounter_discharge_disposition_description
         as
        (

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'DISCHARGE_DISPOSITION_DESCRIPTION' AS field_name
    , case when m.discharge_disposition_description is not null then 'valid' else 'null' end as bucket_name
    , cast(null as TEXT) as invalid_reason
    , cast(substring(discharge_disposition_description, 1, 255) as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
        );
      
  