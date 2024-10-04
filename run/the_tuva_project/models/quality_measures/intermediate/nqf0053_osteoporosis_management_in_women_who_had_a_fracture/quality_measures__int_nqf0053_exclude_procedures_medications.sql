
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_nqf0053_exclude_procedures_medications
         as
        (

with  __dbt__cte__quality_measures__stg_core__procedure as (

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
),  __dbt__cte__quality_measures__stg_pharmacy_claim as (



    select
      cast(null as TEXT ) as patient_id
    , try_cast( null as date ) as dispensing_date
    , cast(null as TEXT ) as ndc_code
    , try_cast( null as date ) as paid_date
    , cast(null as TIMESTAMP ) as tuva_last_run
    limit 0
),  __dbt__cte__quality_measures__stg_core__medication as (


select
      patient_id
    , encounter_id
    , prescribing_date   
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from tuva_synthetic.core.medication


), denominator as (

    select
          patient_id
        , performance_period_begin
    from tuva_synthetic.quality_measures._int_nqf0053_denominator

)

, value_sets as (

    select
          concept_name
        , code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes

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
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from __dbt__cte__quality_measures__stg_core__procedure

)

, pharmacy_claims as (

    select
          patient_id
        , dispensing_date
        , ndc_code
    from __dbt__cte__quality_measures__stg_pharmacy_claim

)

, medications as (

    select
        patient_id
      , encounter_id
      , prescribing_date
      , dispensing_date
      , source_code
      , source_code_type
    from __dbt__cte__quality_measures__stg_core__medication

)

, bone_density_test_codes as (

    select
          concept_name
        , code
        , code_system
    from value_sets
    where lower(concept_name) in (
          'bone mineral density test'
        , 'bone mineral density tests cpt'
        , 'bone mineral density tests hcpcs'
        , 'bone mineral density tests icd10pcs'
        , 'dexa dual energy xray absorptiometry, bone density'
    )

)

, osteoporosis_medication_codes as (

    select
          code
        , code_system
        , concept_name
    from value_sets
    where lower(concept_name) in 
        ( 
          'osteoporosis medications for urology care'
        , 'osteoporosis medication'
        , 'bisphosphonates'
        )

)

, bone_density_test_procedures as (

    select
          procedures.*
        , bone_density_test_codes.concept_name
    from procedures
    inner join bone_density_test_codes
        on procedures.code = bone_density_test_codes.code
            and procedures.code_type = bone_density_test_codes.code_system

)

, osteoporosis_pharmacy_claims as (

    select
        pharmacy_claims.patient_id
      , pharmacy_claims.dispensing_date
      , pharmacy_claims.ndc_code
      , osteoporosis_medication_codes.concept_name
    from pharmacy_claims
    inner join osteoporosis_medication_codes
        on pharmacy_claims.ndc_code = osteoporosis_medication_codes.code
            and lower(osteoporosis_medication_codes.code_system) = 'ndc'
            
)

, osteoporosis_medications as (

    select
        medications.patient_id
      , medications.encounter_id
      , medications.prescribing_date
      , medications.dispensing_date
      , medications.source_code
      , medications.source_code_type
      , osteoporosis_medication_codes.concept_name
    from medications
    inner join osteoporosis_medication_codes
        on medications.source_code = osteoporosis_medication_codes.code
            and medications.source_code_type = osteoporosis_medication_codes.code_system

)

, valid_osteoporosis_medications_procedures as (

    select
          denominator.patient_id
        , osteoporosis_pharmacy_claims.concept_name as exclusion_reason
        , osteoporosis_pharmacy_claims.dispensing_date as exclusion_date
    from denominator
    inner join osteoporosis_pharmacy_claims
        on denominator.patient_id = osteoporosis_pharmacy_claims.patient_id
    where osteoporosis_pharmacy_claims.dispensing_date
        between
            

    dateadd(
        month,
        -12,
        denominator.performance_period_begin
        )


            and denominator.performance_period_begin
    
    union all

    select
          denominator.patient_id
        , osteoporosis_medications.concept_name as exclusion_reason
        , coalesce(osteoporosis_medications.prescribing_date, osteoporosis_medications.dispensing_date) as exclusion_date
    from denominator
    inner join osteoporosis_medications
        on denominator.patient_id = osteoporosis_medications.patient_id
            and coalesce(osteoporosis_medications.prescribing_date, osteoporosis_medications.dispensing_date)
            between
                

    dateadd(
        month,
        -12,
        denominator.performance_period_begin
        )


                and denominator.performance_period_begin

)

, valid_tests_performed as (

    select
          denominator.patient_id
        , bone_density_test_procedures.concept_name as exclusion_reason
        , procedure_date as exclusion_date
    from denominator
    inner join bone_density_test_procedures
        on denominator.patient_id = bone_density_test_procedures.patient_id
    where bone_density_test_procedures.procedure_date
        between 
            

    dateadd(
        year,
        -2,
        denominator.performance_period_begin
        )


            and denominator.performance_period_begin

)

, valid_exclusions as (

    select * from valid_tests_performed

    union all

    select * from valid_osteoporosis_medications_procedures

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , 'measure specific exclusion for procedure medication' as exclusion_type
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from
    valid_exclusions
        );
      
  