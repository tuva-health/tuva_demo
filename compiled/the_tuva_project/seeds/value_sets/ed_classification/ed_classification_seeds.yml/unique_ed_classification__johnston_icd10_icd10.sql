
    
    

select
    icd10 as unique_field,
    count(*) as n_records

from tuva_synthetic.ed_classification._value_set_johnston_icd10
where icd10 is not null
group by icd10
having count(*) > 1


