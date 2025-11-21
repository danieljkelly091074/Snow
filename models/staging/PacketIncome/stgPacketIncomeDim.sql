{{ config(materilized='table')}}
WITH PacketIncome As (

SELECT dbo.Packet.PacketID AS PacketKey, dbo.Packet.PacketNumber, CASE WHEN WebHallNote.PacketNumber IS NOT NULL THEN 1 ELSE 0 END AS OnlinePacket
FROM            {{ source('DBT_SNOWFLAKE', 'PACKET') }} LEFT OUTER JOIN
{{ source('DBT_SNOWFLAKE', 'WEBHALLNOTE') }} ON dbo.Packet.PacketNumber = dbo.WebHallNote.PacketNumber
WHERE       dbo.Packet.COUNTER >= '01-01-2015 00:00:00'
UNION
SELECT dbo.ArchivePacket.PacketID AS PacketKey, dbo.ArchivePacket.PacketNumber, CASE WHEN WebHallNote.PacketNumber IS NOT NULL THEN 1 ELSE 0 END AS OnlinePacket
FROM            {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} LEFT OUTER JOIN
{{ source('DBT_SNOWFLAKE', 'WEBHALLNOTE') }} ON dbo.ArchivePacket.PacketNumber = dbo.WebHallNote.PacketNumber
WHERE       dbo.ArchivePacket.COUNTER >= '01-01-2015 00:00:00'
)

select * from PacketIncome