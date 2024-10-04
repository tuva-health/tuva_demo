select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select icd10
from tuva_synthetic.ed_classification._value_set_johnston_icd10
where icd10 is null



      
    ) dbt_internal_test