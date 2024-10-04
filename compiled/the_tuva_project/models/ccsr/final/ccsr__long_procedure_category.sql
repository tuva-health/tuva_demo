

with  __dbt__cte__ccsr__stg_core__procedure as (


select 
      encounter_id
    , claim_id
    , patient_id
    , normalized_code
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), procedures as (

    select * from __dbt__cte__ccsr__stg_core__procedure

)

, ccsr__procedure_category_map as (

    select * from tuva_synthetic.ccsr.procedure_category_map

)

select distinct
      procedures.encounter_id
    , procedures.claim_id
    , procedures.patient_id
    , procedures.normalized_code
    , ccsr__procedure_category_map.code_description
    , ccsr__procedure_category_map.ccsr_parent_category
    , ccsr__procedure_category_map.ccsr_category
    , ccsr__procedure_category_map.ccsr_category_description
    , ccsr__procedure_category_map.clinical_domain
    , '2023.1' as prccsr_version
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from procedures
left join ccsr__procedure_category_map
    on procedures.normalized_code = ccsr__procedure_category_map.code