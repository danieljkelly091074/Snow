{{ config(materialized='table')}}

WITH SiteDim As (
SELECT *
FROM            {{ source('DBT_SNOWFLAKE', 'SITEDIM') }} 

)

SELECT * FROM SiteDim 