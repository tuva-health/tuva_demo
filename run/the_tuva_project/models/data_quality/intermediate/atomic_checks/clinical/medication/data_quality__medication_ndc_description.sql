
  
    

        create or replace transient table tuva_synthetic.data_quality.medication_ndc_description
         as
        (


SELECT
      m.data_source
    , coalesce(m.dispensing_date,cast('1900-01-01' as date)) as source_date
    , 'MEDICATION' AS table_name
    , 'Medication ID' as drill_down_key
    , coalesce(medication_id, 'NULL') AS drill_down_value
    , 'NDC_DESCRIPTION' as field_name
    , case when term.ndc is not null then 'valid'
           when m.ndc_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.ndc_code is not null and term.ndc is null
           then 'NDC code type does not join to Terminology ndc table'
           else null end as invalid_reason
    , cast(substring(ndc_description, 1, 255) as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.medication m
left join tuva_synthetic.terminology.ndc term on m.ndc_code = term.ndc
        );
      
  