
  
    

        create or replace transient table tuva_synthetic.clinical_concept_library.value_set_member_relevant_fields
         as
        (with value_set_member_relevant_fields as (
select 
  aa.concept_id,
  aa.concept_name,
  aa.concept_type,
  
  bb.value_set_member_id,
  bb.code,
  bb.coding_system_id,
  bb.include_descendants,

  cc.coding_system_name
  
from tuva_synthetic.clinical_concept_library.clinical_concepts aa

left join tuva_synthetic.clinical_concept_library.value_set_members bb
on aa.concept_id = bb.concept_id

left join tuva_synthetic.clinical_concept_library.coding_systems cc
on bb.coding_system_id = cc.coding_system_id
)


select *
from value_set_member_relevant_fields
        );
      
  