
  
    

        create or replace transient table tuva_synthetic.data_quality.condition_present_on_admit_description
         as
        (

SELECT
    m.data_source
    ,coalesce(m.recorded_date,cast('1900-01-01' as date)) as source_date
    ,'CONDITION' AS table_name
    ,'Condition ID' as drill_down_key
    , coalesce(condition_id, 'NULL') AS drill_down_value
    ,'PRESENT_ON_ADMIT_DESCRIPTION' AS field_name
    ,case when m.present_on_admit_description is not null then 'valid' else 'null' end as bucket_name
    ,cast(null as TEXT) as invalid_reason
    ,cast(substring(present_on_admit_description, 1, 255) as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.condition m
        );
      
  