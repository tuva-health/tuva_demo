
  
    

        create or replace transient table tuva_synthetic.data_quality.encounter_ms_drg_code
         as
        (

SELECT
      m.data_source
    , coalesce(m.encounter_start_date,cast('1900-01-01' as date)) as source_date
    , 'ENCOUNTER' AS table_name
    , 'Encounter ID' as drill_down_key
    , coalesce(encounter_id, 'NULL') AS drill_down_value
    , 'MS_DRG_CODE' AS field_name
    , case when term.ms_drg_code is not null then 'valid'
          when m.ms_drg_code is not null then 'invalid'
          else 'null'
    end as bucket_name
    , case when m.ms_drg_code is not null and term.ms_drg_code is null
          then 'MS DRG Code does not join to Terminology ms_drg table'
          else null end as invalid_reason
    , cast(m.ms_drg_code as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.encounter m
left join tuva_synthetic.terminology.ms_drg term on m.ms_drg_code = term.ms_drg_code
        );
      
  