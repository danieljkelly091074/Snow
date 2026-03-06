{{ config(materialized='incremental')}}

WITH CustomerDim As (

SELECT AccountCode, CustomerName, Address1, Address2, Address3, Country, Postcode
FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }}
)

SELECT * FROM CustomerDim