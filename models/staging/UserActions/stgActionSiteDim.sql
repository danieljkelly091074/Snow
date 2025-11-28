{{ config(materilized='table')}}

WITH ActionSiteDim As (
SELECT *
FROM            {{ source('DBT_SNOWFLAKE', 'SITEDIM') }} 

)

SELECT * FROM ActionSiteDim 