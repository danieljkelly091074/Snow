WITH ActionPacket AS (

SELECT DISTINCT dbo.Packet.PacketID, dbo.Packet.PacketNumber, dbo.Packet.TradesmanAccountCode AS AccounCode
FROM            {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} INNER JOIN
                         {{ source('DBT_SNOWFLAKE', 'PACKET') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber
WHERE        (DATEDIFF(day, dbo.SupervisorActions.Date, GETDATE()) <= 1095)
UNION 
SELECT DISTINCT dbo.ArchiveSupervisorActions.PacketID, dbo.ArchivePacket.PacketNumber, dbo.ArchivePacket.TradesmanAccountCode AS AccounCode
FROM             {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} INNER JOIN
                         {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON dbo.ArchiveSupervisorActions.PacketID = dbo.ArchivePacket.PacketID
WHERE        (DATEDIFF(day, dbo.ArchiveSupervisorActions.Date, GETDATE()) <= 1095)


)

select 
*
from ActionPacket
