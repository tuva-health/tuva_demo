

with all_medications as (

    select
          patient_id
        , dispensing_date
        , drug_code
        , code_system
        , data_source
    from tuva_synthetic.hcc_suspecting._int_all_medications

)

, seed_clinical_concepts as (

    select
          concept_name
        , code
        , code_system
    from tuva_synthetic.hcc_suspecting._value_set_clinical_concepts

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
    from tuva_synthetic.hcc_suspecting._value_set_hcc_descriptions

)

, billed_hccs as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , current_year_billed
    from tuva_synthetic.hcc_suspecting._int_patient_hcc_history

)

/* BEGIN HCC 155 logic (Major Depression, Moderate or Severe, without Psychosis)

   antidepressant medication taken within the past five years
*/
, hcc_155_suspect as (

    select
          all_medications.patient_id
        , all_medications.dispensing_date
        , all_medications.drug_code
        , all_medications.code_system
        , all_medications.data_source
        , seed_clinical_concepts.concept_name
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from all_medications
        inner join seed_clinical_concepts
            on all_medications.code_system = seed_clinical_concepts.code_system
            and all_medications.drug_code = seed_clinical_concepts.code
        inner join seed_hcc_descriptions
            on hcc_code = '155'
    where lower(seed_clinical_concepts.concept_name) = 'antidepressant medication'
    and all_medications.dispensing_date >= 

    dateadd(
        year,
        -5,
        convert_timezone('UTC', current_timestamp())
        )



)
/* END HCC 155 logic */

, unioned as (

    select * from hcc_155_suspect

)

, add_billed_flag as (

    select
          unioned.patient_id
        , unioned.data_source
        , unioned.hcc_code
        , unioned.hcc_description
        , unioned.concept_name
        , unioned.dispensing_date
        , unioned.drug_code
        , billed_hccs.current_year_billed
    from unioned
        left join billed_hccs
            on unioned.patient_id = billed_hccs.patient_id
            and unioned.data_source = billed_hccs.data_source
            and unioned.hcc_code = billed_hccs.hcc_code
)

, add_standard_fields as (

    select
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , dispensing_date
        , drug_code
        , current_year_billed
        , cast('Medication suspect' as TEXT) as reason
        , concept_name || drug_code || ') dispensed on ' || dispensing_date as contributing_factor
        , dispensing_date as suspect_date
    from add_billed_flag

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(data_source as TEXT) as data_source
        , cast(hcc_code as TEXT) as hcc_code
        , cast(hcc_description as TEXT) as hcc_description
        , cast(dispensing_date as date) as dispensing_date
        , cast(drug_code as TEXT) as drug_code
        
            , cast(current_year_billed as boolean) as current_year_billed
        
        , cast(reason as TEXT) as reason
        , cast(contributing_factor as TEXT) as contributing_factor
        , cast(suspect_date as date) as suspect_date
    from add_standard_fields

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , dispensing_date
    , drug_code
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types