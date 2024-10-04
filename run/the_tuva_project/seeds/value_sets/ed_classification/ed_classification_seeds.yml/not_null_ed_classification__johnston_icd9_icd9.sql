select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select icd9
from tuva_synthetic.ed_classification._value_set_johnston_icd9
where icd9 is null



      
    ) dbt_internal_test