{{ config(materialized='view') }}
with fhv_trip as 
(select *, 
row_number() over (partition by dispatching_base_num,pickup_datetime, dropoff_datetime) as rnk
from {{ source('staging','fhv_taxirides') }})

SELECT 
{{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime','dropoff_datetime']) }} as tripid,
SAFE_CAST(dispatching_base_num as STRING) as dispatching_base_num,
cast(PUlocationID as integer) as PUlocationID,
cast(DOlocationID as integer) as DOlocationID,
cast(pickup_date AS date) as pickup_date,
cast(dropOff_date AS date) as dropoff_date,
cast(SR_Flag as numeric) as SR_Flag,
cast(Affiliated_base_number as STRING) as Affiliated_base_number
from fhv_trip
WHERE EXTRACT(YEAR from pickup_date)=2019
and rnk=1

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}


