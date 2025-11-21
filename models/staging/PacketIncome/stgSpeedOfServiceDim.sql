{{ config(materilized='table')}}

WITH SpeedService As (
SELECT PacketTypeID As SpeedOfServiceKey, CASE WHEN PacketType LIKE 'Counter' THEN 'Standard' WHEN PacketType LIKE 'Early Hall' THEN 'Next Day' WHEN PacketType LIKE 'Post Early Hall' THEN 'Post Next Day' ELSE PacketType END  
FROM {{ source('DBT_SNOWFLAKE', 'PACKETTYPE') }}
)
select * from SpeedService