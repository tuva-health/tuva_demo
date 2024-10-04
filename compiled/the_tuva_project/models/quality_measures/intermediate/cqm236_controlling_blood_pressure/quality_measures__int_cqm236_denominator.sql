

with  __dbt__cte__quality_measures__stg_core__encounter as (


select
      patient_id
    , encounter_id
    , encounter_type
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.encounter


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
),  __dbt__cte__quality_measures__stg_core__condition as (

select
      patient_id
    , claim_id
    , encounter_id
    , recorded_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.condition
),  __dbt__cte__quality_measures__stg_core__patient as (

select
      patient_id
    , sex
    , birth_date
    , death_date
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from tuva_synthetic.core.patient
), visit_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
          'office visit'
        , 'home healthcare services'
        , 'preventive care services established office visit, 18 and up'
        , 'preventive care services initial office visit, 18 and up'
        , 'annual wellness visit'
        , 'telephone visits'
        , 'emergency department evaluation and management visit'
    )

)

, visits_encounters as (

    select 
           patient_id
         , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
         , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from __dbt__cte__quality_measures__stg_core__encounter encounter
    inner join tuva_synthetic.quality_measures._int_cqm236__performance_period as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
        and  coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    where lower(encounter_type) in (
          'home health'
        , 'outpatient'
        , 'nonacute inpatient'
        , 'encounter inpatient'
        , 'acute_inpatient'
        , 'emergency_department'
    )

)

, procedure_encounters as (

    select 
          patient_id
        , procedure_date as min_date
        , procedure_date as max_date
        from __dbt__cte__quality_measures__stg_core__procedure procs
    inner join tuva_synthetic.quality_measures._int_cqm236__performance_period  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join visit_codes
            on coalesce(procs.normalized_code,procs.source_code) = visit_codes.code

)

, claims_encounters as (

    select 
          patient_id
        , coalesce(claim_start_date,claim_end_date) as min_date
        , coalesce(claim_end_date,claim_start_date) as max_date
    from __dbt__cte__quality_measures__stg_medical_claim medical_claim
    inner join tuva_synthetic.quality_measures._int_cqm236__performance_period  as pp on
        coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
         and coalesce(claim_start_date,claim_end_date) <=  pp.performance_period_end
    inner join visit_codes
        on medical_claim.hcpcs_code= visit_codes.code

)

, all_encounters as (

    select *, 'v' as visit_enc,cast(null as TEXT) as proc_enc, cast(null as TEXT) as claim_enc
    from visits_encounters
    union all
    select *, cast(null as TEXT) as visit_enc, 'p' as proc_enc, cast(null as TEXT) as claim_enc
    from procedure_encounters
    union all
    select *, cast(null as TEXT) as visit_enc,cast(null as TEXT) as proc_enc, 'c' as claim_enc
    from claims_encounters

)

, encounters_by_patient as (

    select
          patient_id
        , min(min_date) min_date
        , max(max_date) max_date
        , concat(concat(
            coalesce(min(visit_enc),'')
            ,coalesce(min(proc_enc),''))
            ,coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by patient_id

)

, hypertension_codes as (

    select
          code
        , code_system
    from tuva_synthetic.quality_measures._value_set_codes
    where lower(concept_name) in (
          'essential hypertension'
    )

)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from __dbt__cte__quality_measures__stg_core__condition

)

, hypertension_conditions as (

    select
          conditions.patient_id
        , conditions.claim_id
        , conditions.encounter_id
        , conditions.recorded_date
        , conditions.source_code
        , conditions.source_code_type
    from conditions
    inner join hypertension_codes
        on coalesce(conditions.normalized_code_type, conditions.source_code_type) = hypertension_codes.code_system
            and coalesce(conditions.normalized_code, conditions.source_code) = hypertension_codes.code

)

, patients_with_age as (
    
    select
          p.patient_id
        , min_date
        , floor(datediff(
        hour,
        birth_date,
        e.min_date
        ) / 8760.0)  as min_age
        , max_date
        , floor(datediff(
        hour,
        birth_date,
        e.max_date
        ) / 8760.0) as max_age
        , qualifying_types
    from __dbt__cte__quality_measures__stg_core__patient p
    inner join encounters_by_patient e
        on p.patient_id = e.patient_id
    where p.death_date is null

)

-- Patients 18-85 years of age who had a visit and diagnosis of essential hypertension starting before and continuing into, or starting during the first six months of the measurement period.
, qualifying_patients as (

    select
        distinct
          hypertension_conditions.patient_id
        , hypertension_conditions.recorded_date
        , patients_with_age.max_age as age
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from hypertension_conditions
    left join patients_with_age
        on hypertension_conditions.patient_id = patients_with_age.patient_id
    cross join tuva_synthetic.quality_measures._int_cqm236__performance_period pp
    where max_age >= 18 and min_age <=  85
        and hypertension_conditions.recorded_date between
            

    dateadd(
        month,
        -12,
        performance_period_begin
        )


            and 
            

    dateadd(
        month,
        6,
        performance_period_begin
        )



)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types