
  
    

        create or replace transient table tuva_synthetic.data_quality.encounter_admit_source_code
         as
        (

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'ADMIT_SOURCE_CODE' AS field_name
    , case when term.admit_source_code is not null then 'valid'
           when m.admit_source_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.admit_source_code is not null and term.admit_source_code is null
           then 'Admit source code does not join to Terminology admit source table'
           else null end as invalid_reason
    , cast(m.admit_source_code as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.terminology.admit_source term on m.admit_source_code = term.admit_source_code
        );
      
  