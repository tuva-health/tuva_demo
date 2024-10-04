


with cte as (
select try_cast( d.source_date as date ) as source_date_type
    ,summary_sk
    ,SUM(CASE WHEN bucket_name = 'valid' THEN 1 ELSE 0 END) as valid_num
    ,SUM(CASE WHEN bucket_name <> 'null' THEN 1 ELSE 0 END) as fill_num
    ,COUNT(drill_down_value) as denom
from tuva_synthetic.data_quality.data_quality_detail d
group by
    try_cast( d.source_date as date )
    ,summary_sk

)

select
      c.first_day_of_month
    , summary_sk
    , sum(valid_num) as valid_num
    , sum(fill_num) as fill_num
    , sum(denom)  as denom
from cte
left join tuva_synthetic.reference_data.calendar c on cte.source_date_type = c.full_date
group by
      c.first_day_of_month
    , summary_sk