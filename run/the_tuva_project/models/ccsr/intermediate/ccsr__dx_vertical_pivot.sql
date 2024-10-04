
  
    

        create or replace transient table tuva_synthetic.ccsr.dx_vertical_pivot
         as
        (

with codes as (
    
    select
        icd_10_cm_code as code, 
        icd_10_cm_code_description as code_description,
        -- loop to generate columns for CCSR categories 1-6
        ccsr_category_1,
        ccsr_category_1_description,
        ccsr_category_2,
        ccsr_category_2_description,
        ccsr_category_3,
        ccsr_category_3_description,
        ccsr_category_4,
        ccsr_category_4_description,
        ccsr_category_5,
        ccsr_category_5_description,
        ccsr_category_6,
        ccsr_category_6_description,
        default_ccsr_category_ip,
        default_ccsr_category_op
    from tuva_synthetic.ccsr._value_set_dxccsr_v2023_1_cleaned_map

), long_union as (
    -- generate select & union statements to pivot category columns to rows
    
    select 
        code,
        code_description,
        substring(ccsr_category_1, 1, 3) as ccsr_parent_category,
        ccsr_category_1 as ccsr_category,
        ccsr_category_1_description as ccsr_category_description,
        1 as ccsr_category_rank,
        CASE WHEN ccsr_category_1 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_1 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    union all
    select 
        code,
        code_description,
        substring(ccsr_category_2, 1, 3) as ccsr_parent_category,
        ccsr_category_2 as ccsr_category,
        ccsr_category_2_description as ccsr_category_description,
        2 as ccsr_category_rank,
        CASE WHEN ccsr_category_2 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_2 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    union all
    select 
        code,
        code_description,
        substring(ccsr_category_3, 1, 3) as ccsr_parent_category,
        ccsr_category_3 as ccsr_category,
        ccsr_category_3_description as ccsr_category_description,
        3 as ccsr_category_rank,
        CASE WHEN ccsr_category_3 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_3 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    union all
    select 
        code,
        code_description,
        substring(ccsr_category_4, 1, 3) as ccsr_parent_category,
        ccsr_category_4 as ccsr_category,
        ccsr_category_4_description as ccsr_category_description,
        4 as ccsr_category_rank,
        CASE WHEN ccsr_category_4 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_4 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    union all
    select 
        code,
        code_description,
        substring(ccsr_category_5, 1, 3) as ccsr_parent_category,
        ccsr_category_5 as ccsr_category,
        ccsr_category_5_description as ccsr_category_description,
        5 as ccsr_category_rank,
        CASE WHEN ccsr_category_5 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_5 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    union all
    select 
        code,
        code_description,
        substring(ccsr_category_6, 1, 3) as ccsr_parent_category,
        ccsr_category_6 as ccsr_category,
        ccsr_category_6_description as ccsr_category_description,
        6 as ccsr_category_rank,
        CASE WHEN ccsr_category_6 = default_ccsr_category_ip THEN 1 ELSE 0 END as is_ip_default_category,
        CASE WHEN ccsr_category_6 = default_ccsr_category_op THEN 1 ELSE 0 END as is_op_default_category
    from codes 
    

)

select distinct
    *,
    '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from long_union
-- as not all diagnosis codes have multiple categories, we can discard nulls
where ccsr_category is not null
        );
      
  