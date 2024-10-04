
  
    

        create or replace transient table tuva_synthetic.data_quality.procedure_procedure_date
         as
        (


SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'PROCEDURE_DATE' as field_name
    , case
        when m.procedure_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'invalid'
        when m.procedure_date <= cast('1901-01-01' as date) then 'invalid'
        when m.procedure_date is null then 'null'
        else 'valid'
    end as bucket_name
    , case
        when m.procedure_date > cast(substring('2024-10-04 19:08:24.316902+00:00',1,10) as date) then 'future'
        when m.procedure_date <= cast('1901-01-01' as date) then 'too old'
        else null
    end as invalid_reason
    , cast(procedure_date as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.procedure m
        );
      
  