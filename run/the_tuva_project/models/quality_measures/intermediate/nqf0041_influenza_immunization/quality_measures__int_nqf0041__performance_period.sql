
  create or replace   view tuva_synthetic.quality_measures._int_nqf0041__performance_period
  
   as (
    /*
    set performance period end to the end of the current calendar year
    or use the quality_measures_period_end variable if provided
      - set quality_measures_period_end to december end for last quarter measurement period
      - set quality_measures_period_end to march end for first quarter measurement period     
*/

with period_end as (

    select
        cast('2018-12-31' as date)
         as performance_period_end
         
)

/*
    set performance period begin to following day of 3 months prior
    for visits in influenza season
*/
, period_begin as (

    select
          performance_period_end
        , 

    dateadd(
        day,
        1,
        

    dateadd(
        month,
        -3,
        performance_period_end
        )


        )

 as performance_period_begin
    from period_end

)

/*
    lookback_period for august of either current or previous year
    for immunization qualifying date
*/
, lookback_period as (

  select
      *
        , case
            when date_part('month', performance_period_end) between 1 and 8
            then (cast(date_part('year', performance_period_end) as integer) - 1) || '-08-01'
            else date_part('year', performance_period_end) || '-08-01'
        end as lookback_period_august
  from period_begin

)

select
      cast((select id
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0041') as TEXT) as measure_id
    , cast((select name
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0041') as TEXT) as measure_name
    , cast((select version
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0041') as TEXT) as measure_version
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
    , cast(lookback_period_august as date) as lookback_period_august
from lookback_period
  );

