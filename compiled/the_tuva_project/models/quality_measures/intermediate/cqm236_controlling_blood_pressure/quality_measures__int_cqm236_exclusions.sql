with frailty as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
  from tuva_synthetic.quality_measures._int_shared_exclusions_frailty
  where exclusion_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)

)

, denominator as (

  select
      patient_id
    , age
  from tuva_synthetic.quality_measures._int_cqm236_denominator

)
-- advanced illness start
, advanced_illness_exclusion as (

  select
    source.*
  from tuva_synthetic.quality_measures._int_shared_exclusions_advanced_illness as source
  inner join frailty
    on source.patient_id = frailty.patient_id
  where source.exclusion_date
    between
      

    dateadd(
        year,
        -1,
        (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)
        )


      and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)

)

, acute_inpatient_advanced_illness as (

  select
    *
  from advanced_illness_exclusion
  where patient_type = 'acute_inpatient'

)

, nonacute_outpatient_advanced_illness as (

  select
    *
  from advanced_illness_exclusion
  where patient_type = 'nonacute_outpatient'

)

, acute_inpatient_counts as (

    select
          patient_id
        , exclusion_type
        , count(distinct exclusion_date) as encounter_count
    from acute_inpatient_advanced_illness
    group by patient_id, exclusion_type

)

, nonacute_outpatient_counts as (

    select
          patient_id
        , exclusion_type
        , count(distinct exclusion_date) as encounter_count
    from nonacute_outpatient_advanced_illness
    group by patient_id, exclusion_type

)

, valid_advanced_illness_exclusions as (

    select
          acute_inpatient_advanced_illness.patient_id
        , acute_inpatient_advanced_illness.exclusion_date
        , acute_inpatient_advanced_illness.exclusion_reason
        , acute_inpatient_advanced_illness.exclusion_type
    from acute_inpatient_advanced_illness
    left join acute_inpatient_counts
      on acute_inpatient_advanced_illness.patient_id = acute_inpatient_counts.patient_id
    where acute_inpatient_counts.encounter_count >= 1

    union all

    select
        nonacute_outpatient_advanced_illness.patient_id
      , nonacute_outpatient_advanced_illness.exclusion_date
      , nonacute_outpatient_advanced_illness.exclusion_reason
      , nonacute_outpatient_advanced_illness.exclusion_type
    from nonacute_outpatient_advanced_illness
    left join nonacute_outpatient_counts
      on nonacute_outpatient_advanced_illness.patient_id = nonacute_outpatient_counts.patient_id
    where nonacute_outpatient_counts.encounter_count >= 2


)
-- advanced illness end

, valid_dementia_exclusions as (

  select
      source.patient_id
    , source.exclusion_date
    , source.exclusion_reason
    , source.exclusion_type
  from tuva_synthetic.quality_measures._int_shared_exclusions_dementia source
  inner join frailty
    on source.patient_id = frailty.patient_id
  where (
    source.dispensing_date
      between 

    dateadd(
        year,
        -1,
        (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)
        )


          and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)
    or source.paid_date
      between 

    dateadd(
        year,
        -1,
        (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)
        )


          and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)
    )

)

, valid_hospice_palliative as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from tuva_synthetic.quality_measures._int_shared_exclusions_hospice_palliative
  where exclusion_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)

)

, valid_institutional_snp as (

  select 
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from tuva_synthetic.quality_measures._int_shared_exclusions_institutional_snp
  where exclusion_date between (
  select 
    performance_period_begin
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

) and (
  select 
    performance_period_end
  from tuva_synthetic.quality_measures._int_cqm236__performance_period

)

)

, measure_specific_procedure_observation_exclusion as (

  select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
    , age
  from tuva_synthetic.quality_measures._int_cqm236_exclude_procedures_observations

)

, exclusions as (

    select *
    from valid_advanced_illness_exclusions
  
    union all

    select *
    from valid_dementia_exclusions

    union all

    select *
    from valid_hospice_palliative

    union all

    select *
    from valid_institutional_snp

    union all

    select *
    from valid_dementia_exclusions

)

, combined_exclusions as (

  select 
      exclusions.*
    , denominator.age
  from exclusions
  inner join denominator
      on exclusions.patient_id = denominator.patient_id

)

, frailty_exclusion_older_than_80 as (

  select
      frailty.patient_id
    , frailty.exclusion_date
    , frailty.exclusion_reason
    , 'measure specific exclusion for patients older than 80' as exclusion_type
    , denominator.age
  from frailty
  inner join denominator
  on frailty.patient_id = denominator.patient_id
    where denominator.age >= 81

)

, valid_exclusions as (

  select * from combined_exclusions
  where exclusion_type != 'hospice_palliative'
    and age >= 66

  union all

  select * from combined_exclusions
  where exclusion_type = 'hospice_palliative'

  union all

  select * from frailty_exclusion_older_than_80

  union all

  select * from measure_specific_procedure_observation_exclusion

)

, add_data_types as (

    select
        distinct
          cast(patient_id as TEXT) as patient_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , 1 as exclusion_flag
    from valid_exclusions

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '2024-10-04 19:11:18.274664+00:00' as tuva_last_run
from add_data_types