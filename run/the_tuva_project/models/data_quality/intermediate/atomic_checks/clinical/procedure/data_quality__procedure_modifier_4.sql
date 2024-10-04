
  
    

        create or replace transient table tuva_synthetic.data_quality.procedure_modifier_4
         as
        (


SELECT
    m.data_source
    ,coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    ,'PROCEDURE' AS table_name
    ,'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    ,'MODIFIER_4' as field_name
    ,case when term.hcpcs is not null then 'valid'
          when m.modifier_4 is not null then 'invalid'
          else 'null'
    end as bucket_name
    ,case when m.modifier_4 is not null and term.hcpcs is null
          then 'Modifier 4 does not join to Terminology hcpcs_level_2 table'
    else null end as invalid_reason
    ,cast(modifier_4 as TEXT) as field_value
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.input_layer.procedure m
left join tuva_synthetic.terminology.hcpcs_level_2 term on m.modifier_4 = term.hcpcs
        );
      
  