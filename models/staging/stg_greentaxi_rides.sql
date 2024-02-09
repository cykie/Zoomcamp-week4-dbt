{{ config(materialized='view') }}

select * from {{ source('staging','green_taxirides')}}  limit 100
