
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_shared_exclusions_advanced_illness
         as
        (

with  __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , encounter_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.condition
),  __dbt__cte__quality_measures__stg_medical_claim as (



select
         cast(null as TEXT ) as patient_id
        , cast(null as TEXT ) as claim_id
        , try_cast( null as date ) as claim_start_date
        , try_cast( null as date ) as claim_end_date
        , cast(null as TEXT ) as place_of_service_code
        , cast(null as TEXT ) as hcpcs_code
        , cast(null as TEXT ) as hcpcs_modifier_1
        , cast(null as TEXT ) as hcpcs_modifier_2
        , cast(null as TEXT ) as hcpcs_modifier_3
        , cast(null as TEXT ) as hcpcs_modifier_4
        , cast(null as TEXT ) as hcpcs_modifier_5
        , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0
),  __dbt__cte__quality_measures__stg_core__procedure as (

select
      patient_id
    , encounter_id
    , procedure_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), patients_with_frailty as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from tuva_synthetic.quality_measures._int_shared_exclusions_frailty

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
          'advanced illness'
        , 'acute inpatient'
        , 'encounter inpatient'
        , 'outpatient'
        , 'observation'
        , 'emergency department visit'
        , 'nonacute inpatient'
    )

)

, conditions as (

    select
          patient_id
        , claim_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__condition

)

, medical_claim as (

    select
          patient_id
        , claim_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from __dbt__cte__quality_measures__stg_medical_claim

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system
    where lower(exclusion_codes.concept_name) = 'advanced illness'

)

, med_claim_exclusions as (

    select
          medical_claim.patient_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

)

, acute_inpatient as (

    select distinct
          patients_with_frailty.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , patients_with_frailty.exclusion_reason || ' with ' || med_claim_exclusions.concept_name || ' and ' || condition_exclusions.concept_name as exclusion_reason
        , med_claim_exclusions.claim_start_date
        , med_claim_exclusions.claim_end_date
        , cast(null as date) as procedure_date
    from patients_with_frailty
         inner join med_claim_exclusions
            on patients_with_frailty.patient_id = med_claim_exclusions.patient_id
         inner join condition_exclusions
            on med_claim_exclusions.claim_id = condition_exclusions.claim_id
    where lower(med_claim_exclusions.concept_name) = 'acute inpatient'

    union all

    select distinct
          patients_with_frailty.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , patients_with_frailty.exclusion_reason || ' with ' || procedure_exclusions.concept_name || ' and ' || condition_exclusions.concept_name as exclusion_reason
        , cast(null as date) as claim_start_date
        , cast(null as date) as claim_end_date
        , procedure_exclusions.procedure_date
    from patients_with_frailty
         inner join procedure_exclusions
         on patients_with_frailty.patient_id = procedure_exclusions.patient_id
         inner join condition_exclusions
         on procedure_exclusions.patient_id = condition_exclusions.patient_id
         and procedure_exclusions.procedure_date = condition_exclusions.recorded_date
    where lower(procedure_exclusions.concept_name) = 'acute inpatient'

)

, nonacute_outpatient as (

    select distinct
          patients_with_frailty.patient_id
        , coalesce(
              med_claim_exclusions.claim_start_date
            , med_claim_exclusions.claim_end_date
          ) as exclusion_date
        , patients_with_frailty.exclusion_reason || ' with ' || med_claim_exclusions.concept_name || ' and ' || condition_exclusions.concept_name as exclusion_reason
        , med_claim_exclusions.claim_start_date
        , med_claim_exclusions.claim_end_date
        , cast(null as date) as procedure_date
    from patients_with_frailty
         inner join med_claim_exclusions
            on patients_with_frailty.patient_id = med_claim_exclusions.patient_id
         inner join condition_exclusions
            on med_claim_exclusions.claim_id = condition_exclusions.claim_id
    where lower(med_claim_exclusions.concept_name) in (
              'encounter inpatient'
            , 'outpatient'
            , 'observation'
            , 'emergency department visit'
            , 'nonacute inpatient'
        )

    union all

    select distinct
          patients_with_frailty.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , patients_with_frailty.exclusion_reason || ' with ' || procedure_exclusions.concept_name || ' and ' || condition_exclusions.concept_name as exclusion_reason
        , cast(null as date) as claim_start_date
        , cast(null as date) as claim_end_date
        , procedure_exclusions.procedure_date
    from patients_with_frailty
         inner join procedure_exclusions
         on patients_with_frailty.patient_id = procedure_exclusions.patient_id
         inner join condition_exclusions
         on procedure_exclusions.patient_id = condition_exclusions.patient_id
         and procedure_exclusions.procedure_date = condition_exclusions.recorded_date
    where lower(procedure_exclusions.concept_name) in (
          'encounter inpatient'
        , 'outpatient'
        , 'observation'
        , 'emergency department visit'
        , 'nonacute inpatient'
    )

)

, exclusions_unioned as (

    select
          acute_inpatient.patient_id
        , acute_inpatient.exclusion_date
        , acute_inpatient.exclusion_reason
        , acute_inpatient.claim_start_date
        , acute_inpatient.claim_end_date
        , acute_inpatient.procedure_date
        , 'acute_inpatient' as patient_type
    from acute_inpatient

    union all

    select
          nonacute_outpatient.patient_id
        , nonacute_outpatient.exclusion_date
        , nonacute_outpatient.exclusion_reason
        , nonacute_outpatient.claim_start_date
        , nonacute_outpatient.claim_end_date
        , nonacute_outpatient.procedure_date
        , 'nonacute_outpatient' as patient_type
    from nonacute_outpatient

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , 'advanced_illness' as exclusion_type
    , claim_start_date
    , claim_end_date
    , procedure_date
    , patient_type
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from exclusions_unioned
        );
      
  