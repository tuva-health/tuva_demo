
  
    

        create or replace transient table tuva_synthetic.quality_measures._int_nqf0034_long
         as
        (with denominator as (

    select
          patient_id
    from tuva_synthetic.quality_measures._int_nqf0034_denominator

)

, numerator as (

    select
          patient_id
        , evidence_date
    from tuva_synthetic.quality_measures._int_nqf0034_numerator

)

, exclusions as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from tuva_synthetic.quality_measures._int_nqf0034_exclusions

)

, measure_flags as (

    select
          denominator.patient_id
        , case
            when denominator.patient_id is not null
            then 1
            else null
          end as denominator_flag
        , case
            when numerator.patient_id is not null and denominator.patient_id is not null
            then 1
            when denominator.patient_id is not null
            then 0
            else null
          end as numerator_flag
        , case
            when exclusions.patient_id is not null and denominator.patient_id is not null
            then 1
            when denominator.patient_id is not null
            then 0
            else null
          end as exclusion_flag
        , numerator.evidence_date
        , exclusions.exclusion_date
        , exclusions.exclusion_reason
        , pp.performance_period_begin
        , pp.performance_period_end
        , (
    select id
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0034'
    )  as measure_id
        , (

    select name
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0034'

    )  as measure_name
        , (
    select version
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0034'

    )  as measure_version
    from denominator
    inner join tuva_synthetic.quality_measures._int_nqf0034__performance_period pp
        on 1 = 1
        left join numerator
            on denominator.patient_id = numerator.patient_id
        left join exclusions
            on denominator.patient_id = exclusions.patient_id

)

/*
    Deduplicate measure rows by latest evidence date or exclusion date
*/
, add_rownum as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , row_number() over(
            partition by
                  patient_id
                , performance_period_begin
                , performance_period_end
                , measure_id
                , measure_name
              order by
                  case when evidence_date is null then 1 else 0 end,
                  evidence_date desc
                , case when exclusion_date is null then 1 else 0 end,
                  exclusion_date desc
          ) as row_num
    from measure_flags

)

, deduped as (

    select
          patient_id
        , denominator_flag
        , numerator_flag
        , exclusion_flag
        , evidence_date
        , exclusion_date
        , exclusion_reason
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from add_rownum
    where row_num = 1

)

, add_data_types as (

    select
          cast(patient_id as TEXT) as patient_id
        , cast(denominator_flag as integer) as denominator_flag
        , cast(numerator_flag as integer) as numerator_flag
        , cast(exclusion_flag as integer) as exclusion_flag
        , cast(evidence_date as date) as evidence_date
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as TEXT) as exclusion_reason
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as TEXT) as measure_id
        , cast(measure_name as TEXT) as measure_name
        , cast(measure_version as TEXT) as measure_version
    from deduped

)

select
      patient_id
    , denominator_flag
    , numerator_flag
    , exclusion_flag
    , evidence_date
    , cast(null as TEXT) as evidence_value
    , exclusion_date
    , exclusion_reason
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , '2024-10-04 19:08:24.316902+00:00' as tuva_last_run
from add_data_types
        );
      
  