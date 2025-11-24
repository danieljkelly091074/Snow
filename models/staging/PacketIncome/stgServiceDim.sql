{{ config(materilized='table')}}

WITH Service As (
SELECT ServiceID As ServiceKey, ServiceDescription, ExternalID, ServiceType 
FROM {{ source('DBT_SNOWFLAKE', 'SERVICEDIM') }}
)

SELECT * FROM Service 