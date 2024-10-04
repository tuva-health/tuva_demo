
  
    

        create or replace transient table tuva_synthetic.ccsr.procedure_category_map
         as
        (

select 
    icd_10_pcs as code,
    icd_10_pcs_description as code_description,
    prccsr as ccsr_category,
    substring(prccsr, 1, 3) as ccsr_parent_category,
    prccsr_description as ccsr_category_description,
    clinical_domain,
   '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.ccsr._value_set_prccsr_v2023_1_cleaned_map
        );
      
  