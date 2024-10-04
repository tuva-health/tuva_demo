
  create or replace   view tuva_synthetic.quality_measures._int_cbe0101__performance_period
  
   as (
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
    set performance period begin to following day of 1 year prior
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

select
      cast((select id
from tuva_synthetic.quality_measures._value_set_measures
where id = 'CBE0101') as TEXT) as measure_id
    , cast((select name
from tuva_synthetic.quality_measures._value_set_measures
where id = 'CBE0101') as TEXT) as measure_name
    , cast((select version
from tuva_synthetic.quality_measures._value_set_measures
where id = 'CBE0101') as TEXT) as measure_version
    , cast(performance_period_begin as date) as performance_period_begin
    , cast(performance_period_end as date) as performance_period_end
from period_begin
  );

