{{ config(materilized='table')}}

WITH ActionPacket AS (

SELECT DISTINCT dbo.Packet.PacketID AS PacketKey, dbo.Packet.PacketNumber, dbo.Packet.TradesmanAccountCode AS AccountCode, dbo.Packet.AmountGross 
FROM                     {{ source('DBT_SNOWFLAKE', 'PACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber
WHERE        (DATEDIFF(day, dbo.SupervisorActions.Date, GETDATE()) <= 1095)
UNION 
SELECT DISTINCT dbo.ArchiveSupervisorActions.PacketID AS PacketKey, dbo.ArchivePacket.PacketNumber, dbo.ArchivePacket.TradesmanAccountCode AS AccountCode, dbo.ArchivePacket.AmountGross
FROM             {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} INNER JOIN
                         {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON dbo.ArchiveSupervisorActions.PacketID = dbo.ArchivePacket.PacketID
WHERE        (DATEDIFF(day, dbo.ArchiveSupervisorActions.Date, GETDATE()) <= 1095)


)

select 
*
from ActionPacket
