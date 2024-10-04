/*
    set performance period end to the end of the current calendar year
    or use the quality_measures_period_end variable if provided
*/
with period_end as (

    select
        cast('2018-12-31' as date)
         as performance_period_end
)

/*
    set performance period begin to a year and a day prior
    for a complete calendar year
*/
, period_begin as (

    select
          performance_period_end
        , 

    dateadd(
        day,
        1,
        

    dateadd(
        year,
        -1,
        performance_period_end
        )


        )

 as performance_period_begin
    from period_end

)

-- lookback_period for last june 30 and december 31
, lookback_period as (

SELECT
  *
  , case
      when performance_period_end >= cast((date_part('year', performance_period_end) || '-06-30') as date)
      then date_part('year', performance_period_end) || '-06-30'
      else date_part('year', performance_period_begin) || '-06-30'
    end as lookback_period_june
  , case
      when performance_period_end >= cast((date_part('year', performance_period_end) || '-12-31') as date)
      then date_part('year', performance_period_end) || '-12-31'
      else date_part('year', performance_period_begin) || '-12-31'
    end as lookback_period_december
FROM period_begin

)

select
      cast((select id
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0053') as TEXT) as measure_id
    , cast((select name
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0053') as TEXT) as measure_name
    , cast((select version
from tuva_synthetic.quality_measures._value_set_measures
where id = 'NQF0053') as TEXT) as measure_version
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
    , cast(lookback_period_june as date) as lookback_period_june
    , cast(lookback_period_december as date) as lookback_period_december
from lookback_period