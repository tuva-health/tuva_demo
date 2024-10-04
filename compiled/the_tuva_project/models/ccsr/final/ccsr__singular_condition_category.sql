

select 
    encounter_id,
    claim_id,
    patient_id,
    ccsr_category,
    ccsr_category_description,
    ccsr_parent_category,
    parent_category_description,
    body_system,
    '2023.1' as dxccsr_version,
    '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.ccsr.long_condition_category
where
        is_ip_default_category = 1
        and condition_rank = 1