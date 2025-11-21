{{ config(materilized='table')}}
WITH UnionService As (
SELECT ServiceKey, dbo.Packet.PacketNumber, dbo.Packet.PacketID, dbo.PacketAdditionalService.InvoiceQuantity AS Quantity, dbo.PacketAdditionalService.ServicePricePerItem AS Price, 
FROM {{ ref('stgServiceDim') }}  INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKETADDITIONALSERVICE') }} ON EXTERNALID = ADDITIONALSERVICEID INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKET') }}  ON DBO.PACKET.PACKETNUMBER = DBO.PACKETADDITIONALSERVICE.PACKETNUMBER 
WHERE       dbo.Packet.Opened >= '01-01-2015 00:00:00' AND dbo.Packet.Opened IS NOT NULL
UNION 
SELECT ServiceKey, dbo.Packet.PacketNumber, dbo.Packet.PacketID, dbo.PacketItem.InvoiceQuantity AS Quantity, dbo.PacketItem.ItemPricePerItem AS Price  
FROM {{ ref('stgServiceDim') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKETITEM') }} ON EXTERNALID = ITEMCODE INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKET') }}  ON DBO.PACKET.PACKETNUMBER = DBO.PACKETITEM.PACKETNUMBER
WHERE       dbo.Packet.Opened >= '01-01-2015 00:00:00' AND dbo.Packet.Opened IS NOT NULL
UNION
SELECT  ServiceKey, dbo.ArchivePacket.PacketNumber, dbo.ArchivePacket.PacketID, dbo.ArchivePacketAdditionalService.InvoiceQuantity AS Quantity, 
                         dbo.ArchivePacketAdditionalService.ServicePricePerItem AS Price, 
FROM {{ ref('stgServiceDim') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKETADDITIONALSERVICE') }} ON EXTERNALID = ADDITIONALSERVICEID INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON DBO.ARCHIVEPACKET.PacketID = DBO.ARCHIVEPACKETADDITIONALSERVICE.PacketID
WHERE       dbo.ArchivePacket.Opened >= '01-01-2015 00:00:00' AND dbo.ArchivePacket.Opened IS NOT NULL
UNION
SELECT ServiceKey, dbo.ArchivePacket.PacketNumber, dbo.ArchivePacket.PacketID, dbo.ArchivePacketItem.InvoiceQuantity AS Quantity, dbo.ArchivePacketItem.ItemPricePerItem AS Price 
FROM {{ ref('stgServiceDim') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKETITEM') }} ON EXTERNALID = ITEMCODE INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON DBO.ARCHIVEPACKET.PACKETID = DBO.ARCHIVEPACKETITEM.PACKETID
WHERE       dbo.ArchivePacket.Opened >= '01-01-2015 00:00:00' AND dbo.ArchivePacket.Opened IS NOT NULL

)

SELECT * FROM UnionService