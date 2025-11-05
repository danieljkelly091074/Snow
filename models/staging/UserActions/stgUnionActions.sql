{{ config(materilized='table')}}

WITH ActionUnion AS (

SELECT DISTINCT PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Marked' AS ActionDesc, MarkedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'PACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.Packet.PacketID   
WHERE Action LIKE '%Marked%'
UNION
SELECT DISTINCT PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Opened' AS ActionDesc, OpenedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'PACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.Packet.PacketID
WHERE Action LIKE 'Opened%'
UNION
SELECT DISTINCT PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Packed' AS ActionDesc, PackedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'PACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.Packet.PacketID
WHERE Action LIKE '%W. Prior to Packing%'
UNION
SELECT DISTINCT PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Sampled' AS ActionDesc, SampledAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                    {{ source('DBT_SNOWFLAKE', 'PACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }} ON dbo.SupervisorActions.PacketNumber = dbo.Packet.PacketNumber INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.Packet.PacketID
WHERE Action LIKE 'Sample%'
UNION
SELECT DISTINCT ARCHIVEPACKET.PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Marked' AS ActionDesc, MarkedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} ON dbo.ARCHIVESupervisorActions.PacketID = dbo.ArchivePacket.PacketID INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.ArchivePacket.PacketID   
WHERE Action LIKE '%Marked%'
UNION
SELECT DISTINCT ARCHIVEPACKET.PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Opened' AS ActionDesc, MarkedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} ON dbo.ARCHIVESupervisorActions.PacketID = dbo.ArchivePacket.PacketID INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.ArchivePacket.PacketID   
WHERE Action LIKE 'Opened%'
UNION
SELECT DISTINCT ARCHIVEPACKET.PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Packed' AS ActionDesc, MarkedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} ON dbo.ARCHIVESupervisorActions.PacketID = dbo.ArchivePacket.PacketID INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.ArchivePacket.PacketID   
WHERE Action LIKE '%W. Prior to Packing%'
UNION
SELECT DISTINCT ARCHIVEPACKET.PACKETID, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, 'Sampled' AS ActionDesc, MarkedAt As SiteDesc, UserName, NoOfItemsEnteredByCPC As TotalItems, NoofSampled As TotalSampled
FROM                     {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }} ON dbo.ARCHIVESupervisorActions.PacketID = dbo.ArchivePacket.PacketID INNER JOIN  RAW__FORGE__AOPC.DBO.STGPACKETDIM ON DBO.STGPACKETDIM.PacketKey = dbo.ArchivePacket.PacketID   
WHERE Action LIKE 'Sample%'

)

select 
*
from ActionUnion
