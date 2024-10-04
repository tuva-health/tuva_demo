
    
    

select
    lab_result_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.lab_result
where lab_result_id is not null
group by lab_result_id
having count(*) > 1


