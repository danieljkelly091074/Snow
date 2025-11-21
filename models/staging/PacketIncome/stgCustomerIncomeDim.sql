{{ config(materilized='table')}}

WITH CustomerIncome As (

SELECT AccountCode AS CustomerKey, CustomerName, CASE WHEN Nationality = 1 THEN 'British' ELSE 'Foreign' END AS Nationality, Postcode
FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }}
)

SELECT * FROM CustomerIncome