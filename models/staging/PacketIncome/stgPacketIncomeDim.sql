{{ config(materialized='table')}}
WITH PacketIncome As (

SELECT dbo.Packet.PacketID AS PacketKey, dbo.Packet.PacketNumber, CASE WHEN WebHallNote.PacketNumber IS NOT NULL THEN 1 ELSE 0 END AS OnlinePacket
FROM            {{ source('DBT_SNOWFLAKE', 'PACKET') }} LEFT OUTER JOIN
{{ source('DBT_SNOWFLAKE', 'WEBHALLNOTE') }} ON dbo.Packet.PacketNumber = dbo.WebHallNote.PacketNumber
WHERE       dbo.PACKET.COUNTER >= TO_TIMESTAMP('01-01-2015 00:00:00', 'DD-MM-YYYY HH24:MI:SS')
UNION
SELECT dbo.ArchivePacket.PacketID AS PacketKey, dbo.ArchivePacket.PacketNumber, CASE WHEN WebHallNote.PacketNumber IS NOT NULL THEN 1 ELSE 0 END AS OnlinePacket
FROM            {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} LEFT OUTER JOIN
{{ source('DBT_SNOWFLAKE', 'WEBHALLNOTE') }} ON dbo.ArchivePacket.PacketNumber = dbo.WebHallNote.PacketNumber
WHERE       dbo.ARCHIVEPACKET.COUNTER  >= TO_TIMESTAMP('01-01-2015 00:00:00', 'DD-MM-YYYY HH24:MI:SS')
)

select * from PacketIncome