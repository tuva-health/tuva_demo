
  
    

        create or replace transient table tuva_synthetic.hcc_suspecting._int_comorbidity_suspects
         as
        (

with conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from tuva_synthetic.hcc_suspecting._int_prep_conditions

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

/* BEGIN HCC 37 logic */
, ckd_stage_1_or_2 as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
        , row_number() over (
            partition by
                  conditions.patient_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) in (
          'chronic kidney disease, stage 1'
        , 'chronic kidney disease, stage 2'
    )

)

, ckd_stage_1_or_2_dedupe as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from ckd_stage_1_or_2
    where row_num = 1

)

, diabetes as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
        , row_number() over (
            partition by
                  conditions.patient_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) = 'diabetes'
)

, diabetes_dedupe as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from diabetes
    where row_num = 1

)

, hcc_37_suspect as (

    select
          diabetes_dedupe.patient_id
        , diabetes_dedupe.data_source
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
        , diabetes_dedupe.concept_name as condition_1_concept_name
        , diabetes_dedupe.code as condition_1_code
        , diabetes_dedupe.recorded_date as condition_1_recorded_date
        , ckd_stage_1_or_2_dedupe.concept_name as condition_2_concept_name
        , ckd_stage_1_or_2_dedupe.code as condition_2_code
        , ckd_stage_1_or_2_dedupe.recorded_date as condition_2_recorded_date
    from diabetes_dedupe
        inner join ckd_stage_1_or_2_dedupe
            on diabetes_dedupe.patient_id = ckd_stage_1_or_2_dedupe.patient_id
            and diabetes_dedupe.data_source = ckd_stage_1_or_2_dedupe.data_source
            /* ensure conditions overlap in the same year */
            and date_part('year', diabetes_dedupe.recorded_date) = date_part('year', ckd_stage_1_or_2_dedupe.recorded_date)
        inner join seed_hcc_descriptions
            on hcc_code = '37'

)
/* END HCC 37 logic */

, unioned as (

    select * from hcc_37_suspect

)

, add_billed_flag as (

    select
          unioned.patient_id
        , unioned.data_source
        , unioned.hcc_code
        , unioned.hcc_description
        , unioned.condition_1_concept_name
        , unioned.condition_1_code
        , unioned.condition_1_recorded_date
        , unioned.condition_2_concept_name
        , unioned.condition_2_code
        , unioned.condition_2_recorded_date
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
        , condition_1_concept_name
        , condition_1_code
        , condition_1_recorded_date
        , condition_2_concept_name
        , condition_2_code
        , condition_2_recorded_date
        , current_year_billed
        , cast('Comorbidity suspect' as TEXT) as reason
        , condition_1_concept_name || ' (' || condition_1_code || ') on ' || condition_1_recorded_date || ') and ' || condition_2_concept_name || ' (' || condition_2_code || ') on ' || condition_2_recorded_date as contributing_factor
        , condition_1_recorded_date as suspect_date
    from add_billed_flag

)


, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(data_source as TEXT) as data_source
        , cast(hcc_code as TEXT) as hcc_code
        , cast(hcc_description as TEXT) as hcc_description
        , cast(condition_1_concept_name as TEXT) as condition_1_concept_name
        , cast(condition_1_code as TEXT) as condition_1_code
        , cast(condition_1_recorded_date as date) as condition_1_recorded_date
        , cast(condition_2_concept_name as TEXT) as condition_2_concept_name
        , cast(condition_2_code as TEXT) as condition_2_code
        , cast(condition_2_recorded_date as date) as condition_2_recorded_date
        
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
    , condition_1_concept_name
    , condition_1_code
    , condition_1_recorded_date
    , condition_2_concept_name
    , condition_2_code
    , condition_2_recorded_date
    , current_year_billed
    , reason
    , contributing_factor
    , suspect_date
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  