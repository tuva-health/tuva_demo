
  
    

        create or replace transient table tuva_synthetic.data_quality.procedure_practitioner_id
         as
        (


SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'PRACTITIONER_ID' AS field_name
    , case when m.practitioner_id is not null then 'valid' else 'null' end as bucket_name
    , cast(null as TEXT) as invalid_reason
    , cast(practitioner_id as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.procedure m
        );
      
  