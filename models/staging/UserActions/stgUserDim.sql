{{ config(materilized='table')}}

WITH Action AS (
    SELECT dbo.USER_DIM.USERKEY, dbo.USER_DIM.USERNAME
    FROM {{ source('DBT_SNOWFLAKE', 'USER_DIM') }}   
)

select 
*
from Action
