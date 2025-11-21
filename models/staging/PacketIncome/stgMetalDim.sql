{{ config(materilized='table')}}

WITH MetalIncome As (
SELECT MetalCode As MetalKey, MetalDescription 
FROM {{ source('DBT_SNOWFLAKE', 'METAL') }}
)

SELECT * FROM MetalIncome