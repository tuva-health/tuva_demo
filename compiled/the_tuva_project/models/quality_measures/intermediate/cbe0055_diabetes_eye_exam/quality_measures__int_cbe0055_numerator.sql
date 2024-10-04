

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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from tuva_synthetic.quality_measures._int_cbe0055_denominator

)

, retina_test_code as (

    select
          code
        , code_system
        , concept_name
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in  (
          'diabetic retinal eye exam met'
        , 'retinal or dilated eye exam'
    )

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

, qualifying_patients as (

    select 
          procedures.patient_id
        , procedures.procedure_date as evidence_date
    from procedures
    inner join tuva_synthetic.quality_measures._int_cbe0055__performance_period pp
    on procedure_date between
        performance_period_begin and performance_period_end
    inner join retina_test_code
        on procedures.code = retina_test_code.code
            and procedures.code_type = retina_test_code.code_system

)

, qualifying_patients_with_denominator as (

    select 
          qualifying_patients.patient_id
        , qualifying_patients.evidence_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , cast('1' as integer) as numerator_flag
    from qualifying_patients
    inner join denominator
    on qualifying_patients.patient_id = denominator.patient_id

)

, add_data_types as (

     select distinct
          cast(patient_id as TEXT) as patient_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(null as TEXT) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from qualifying_patients_with_denominator

)

select
      patient_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
from add_data_types