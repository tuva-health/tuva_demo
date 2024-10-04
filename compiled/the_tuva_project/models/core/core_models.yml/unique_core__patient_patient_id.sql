
    
    

select
    patient_id as unique_field,
    count(*) as n_records

from tuva_synthetic.core.patient
where patient_id is not null
group by patient_id
having count(*) > 1


