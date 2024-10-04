

with  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.patient
),  __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.encounter


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
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.procedure
), performance_period as (

    select
          measure_id
        , measure_name
        , measure_version
        , performance_period_end
        , performance_period_begin
        , performance_period_lookback
    from tuva_synthetic.quality_measures._int_nqf2372__performance_period

)

, patient as (

    select
          patient_id
        , sex
        , birth_date
        , death_date
    from __dbt__cte__quality_measures__stg_core__patient

)

, encounters as (

    select
          patient_id
        , encounter_type
        , encounter_start_date
    from __dbt__cte__quality_measures__stg_core__encounter

)

, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
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

, visit_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where concept_name in (
          'Office Visit'
        , 'Home Healthcare Services'
        , 'Preventive Care Services Established Office Visit, 18 and Up'
        , 'Preventive Care Services Initial Office Visit, 18 and Up'
        , 'Annual Wellness Visit'
        , 'Telephone Visits'
        , 'Online Assessments'
    )

)

, patient_with_age as (

    select
          patient.patient_id
        , patient.sex
        , patient.birth_date
        , patient.death_date
        , performance_period.measure_id
        , performance_period.measure_name
        , performance_period.measure_version
        , performance_period.performance_period_begin
        , performance_period.performance_period_end
        , performance_period.performance_period_lookback
        , floor(datediff(
        hour,
        patient.birth_date,
        performance_period.performance_period_begin
        ) / 8760.0) as age  -- 365*24 hours in a year
    from patient
         cross join performance_period

)

/*
    Filter patient to living women 51 - 74 years of age
    at the beginning of the measurement period
*/
, patient_filtered as (

    select
          patient_id
        , age
        , measure_id
        , measure_name
        , measure_version
        , performance_period_begin
        , performance_period_end
        , performance_period_lookback
        , 1 as denominator_flag
    from patient_with_age
    where lower(sex) = 'female'
        and age between 51 and 74
        and death_date is null

)

/*
    Filter to qualifying visit types by claim procedures
*/
, visit_claims as (

    select
          medical_claim.patient_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
    from medical_claim
         inner join visit_codes
            on medical_claim.hcpcs_code = visit_codes.code
    where visit_codes.code_system = 'hcpcs'

)

/*
    Filter encounters to qualifying visit type
*/
, visit_encounters as (

    select
          patient_id
        , encounter_start_date
    from encounters
    where lower(encounter_type) in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
        )

)

/*
    Filter to qualifying visit types by procedure
*/
, visit_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
    from procedures
         inner join visit_codes
             on procedures.code = visit_codes.code
             and procedures.code_type = visit_codes.code_system

)

/*
    Filter to final eligible population/denominator before exclusions
    with a qualifying visit during the measurement period
*/
, eligible_population as (

    select
          patient_filtered.patient_id
        , patient_filtered.age
        , patient_filtered.measure_id
        , patient_filtered.measure_name
        , patient_filtered.measure_version
        , patient_filtered.performance_period_begin
        , patient_filtered.performance_period_end
        , performance_period_lookback
        , patient_filtered.denominator_flag
    from patient_filtered
         left join visit_claims
            on patient_filtered.patient_id = visit_claims.patient_id
         left join visit_procedures
            on patient_filtered.patient_id = visit_procedures.patient_id
         left join visit_encounters
            on patient_filtered.patient_id = visit_encounters.patient_id
    where (
        visit_claims.claim_start_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_claims.claim_end_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_procedures.procedure_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
        or visit_encounters.encounter_start_date
            between patient_filtered.performance_period_begin
            and patient_filtered.performance_period_end
    )

)

, add_data_types as (

    select distinct
          cast(patient_id as TEXT) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(performance_period_lookback as date) as performance_period_lookback
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from eligible_population

)

 select distinct
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , performance_period_lookback
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types