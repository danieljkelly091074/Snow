{{ config(materilized='table')}}
WITH PacketIncomeFact As (
SELECT dbo.Packet.PacketID As PacketKey, TO_CHAR(dbo.Packet.Opened, 'YYYYMMDD')::INT AS DateKey, PacketTypeID AS SpeedOfServiceKey, ServiceKey, 
TradesmanAccountCode As CustomerKey, dbo.Packet.MetalCode As MetalKey, SiteKey, Quantity, Price, dbo.Packet.VATRate      
    FROM {{ source('DBT_SNOWFLAKE', 'PACKET') }} 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'METAL') }} ON dbo.Packet.MetalCode = dbo.Metal.MetalCode
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKETTYPE') }} ON dbo.Packet.PacketType = dbo.PacketType.PacketType 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} ON dbo.Packet.TradesmanAccountCode = dbo.Customer.AccountCode 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'SITEDIM') }} ON dbo.Packet.OpenedAt = dbo.SiteDim.SiteDescription 
    INNER JOIN {{ ref('stgUnionService') }} ON dbo.Packet.PacketID = stgUnionService.PacketID
WHERE       dbo.Packet.Opened >= '01-01-2015 00:00:00' AND dbo.Packet.Opened IS NOT NULL
UNION
SELECT dbo.ArchivePacket.PacketID As PacketKey, TO_CHAR(dbo.ArchivePacket.Opened, 'YYYYMMDD')::INT AS DateKey, PacketTypeID AS SpeedOfServiceKey, ServiceKey, 
TradesmanAccountCode As CustomerKey, dbo.ArchivePacket.MetalCode As MetalKey, SiteKey, Quantity, Price, dbo.ArchivePacket.VATRate      
    FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'METAL') }} ON dbo.ArchivePacket.MetalCode = dbo.Metal.MetalCode
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKETTYPE') }} ON dbo.ArchivePacket.PacketType = dbo.PacketType.PacketType 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} ON dbo.ArchivePacket.TradesmanAccountCode = dbo.Customer.AccountCode 
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'SITEDIM') }} ON dbo.ArchivePacket.OpenedAt = dbo.SiteDim.SiteDescription 
    INNER JOIN {{ ref('stgUnionService') }} ON dbo.ArchivePacket.PacketID = stgUnionService.PacketID
WHERE       dbo.ArchivePacket.Opened >= '01-01-2015 00:00:00' AND dbo.ArchivePacket.Opened IS NOT NULL
)
SELECT * FROM PacketIncomeFact