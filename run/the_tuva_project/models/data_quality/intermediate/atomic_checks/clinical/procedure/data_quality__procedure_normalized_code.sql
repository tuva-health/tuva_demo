
  
    

        create or replace transient table tuva_synthetic.data_quality.procedure_normalized_code
         as
        (

with icd9 as (
    SELECT
          m.data_source
        , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
        , 'PROCEDURE' AS table_name
        , 'Procedure ID' as drill_down_key
        , coalesce(procedure_id, 'NULL') AS drill_down_value
        , 'NORMALIZED_CODE' as field_name
        , case when term.icd_9_pcs is not null then 'valid'
               when m.normalized_code is not null then 'invalid'
               else 'null'
        end as bucket_name
        , case when m.normalized_code is not null and term.icd_9_pcs is null
               then 'Normalized code does not join to Terminology icd_9_pcs table'
               else null end as invalid_reason
        , cast(normalized_code as TEXT) as field_value
    from tuva_synthetic.input_layer.procedure m
    left join tuva_synthetic.terminology.icd_9_pcs term on m.normalized_code = term.icd_9_pcs
    where
        m.normalized_code_type = 'icd-9-pcs'
),
icd10 as (
    SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.icd_10_pcs is not null then 'valid'
           when m.normalized_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.icd_10_pcs is null
           then 'Normalized code does not join to Terminology icd_10_pcs table'
           else null end as invalid_reason
    , cast(normalized_code as TEXT) as field_value
from tuva_synthetic.input_layer.procedure m
left join tuva_synthetic.terminology.icd_10_pcs term on m.normalized_code = term.icd_10_pcs
where
    m.normalized_code_type = 'icd_10_pcs'
),
hcpcs_level_2 as (
    SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , case when term.hcpcs is not null then 'valid'
           when m.normalized_code is not null then 'invalid'
           else 'null'
    end as bucket_name
    , case when m.normalized_code is not null and term.hcpcs is null
           then 'Normalized code does not join to Terminology hcpcs_level_2 table'
           else null end as invalid_reason
    , cast(normalized_code as TEXT) as field_value
from tuva_synthetic.input_layer.procedure m
left join tuva_synthetic.terminology.hcpcs_level_2 term on m.normalized_code = term.hcpcs
where
    m.normalized_code_type = 'hcpcs_level_2'
),

others as (
    SELECT
      m.data_source
    , coalesce(m.procedure_date,cast('1900-01-01' as date)) as source_date
    , 'PROCEDURE' AS table_name
    , 'Procedure ID' as drill_down_key
    , coalesce(procedure_id, 'NULL') AS drill_down_value
    , 'NORMALIZED_CODE' as field_name
    , 'null' as bucket_name
    , 'code type does not have a matching code terminology table' as invalid_reason
    , cast(normalized_code as TEXT) as field_value
from tuva_synthetic.input_layer.procedure m
where
    m.normalized_code_type not in ('icd-9-pcs', 'icd-10-pcs','hcpcs_level_2')
)

SELECT *, '2024-10-04 19:08:24.316902+00:00' as tuva_last_run FROM icd9

union all

SELECT * , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run FROM icd10

union all

SELECT * , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run FROM hcpcs_level_2

union all

SELECT * , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run FROM others
        );
      
  