
  
    

        create or replace transient table tuva_synthetic.data_quality.lab_result_normalized_component
         as
        (


SELECT
      m.data_source
    , coalesce(m.result_date,cast('1900-01-01' as date)) as source_date
    , 'LAB_RESULT' AS table_name
    , 'Lab Result ID' as drill_down_key
    , coalesce(lab_result_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_COMPONENT' as field_name
    , case when term.loinc is not null then 'valid'
          when m.normalized_component is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.normalized_component is not null and term.loinc is null
           then 'Normalized component does not join to Terminology loinc table'
           else null end as invalid_reason
    , cast(normalized_component as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.lab_result m
left join tuva_synthetic.terminology.loinc term on m.normalized_component = term.loinc
        );
      
  