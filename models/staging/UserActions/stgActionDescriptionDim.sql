{{ config(materilized='table')}}

WITH ActionDescription AS (
SELECT * FROM {{ source('DBT_SNOWFLAKE', 'ACTIONDIM') }} 

)
SELECT * FROM ActionDescription