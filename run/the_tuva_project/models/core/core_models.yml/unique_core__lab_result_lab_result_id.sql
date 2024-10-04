select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    lab_result_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.lab_result
where lab_result_id is not null
group by lab_result_id
having count(*) > 1



      
    ) dbt_internal_test