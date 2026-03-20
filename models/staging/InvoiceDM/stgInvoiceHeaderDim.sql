--{{ config(materialized='incremental')}}

--WITH HeaderDim As (

--SELECT dbo.Packet.PacketID As HeaderID, InvoiceNumber, concat('Your Ref:', ClientPacketNumber) AS ClientRef, SUM(Amount) AS PaymentAmount, dbo.Packet.PacketNumber, PacketType, CustomerVAT, ZeroVatMessage, current_date() as created_at
--FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKET') }} ON dbo.Customer.AccountCode = dbo.Packet.TradesmanAccountCode
--LEFT OUTER JOIN {{ source('DBT_SNOWFLAKE', 'PAYMENTS') }} ON dbo.Packet.PacketNumber = dbo.Payments.PacketNumber
--GROUP BY dbo.Packet.PacketNumber, LeftHall, InvoiceNumber, dbo.Packet.PacketID, Packed, ClientPacketNumber, WithdrawalDateTime, PacketType, 
                         CustomerVAT, ZeroVatMessage, SentToSage
--HAVING        (SentToSage IS NOT NULL) AND (WithdrawalDateTime IS NULL) AND (InvoiceNumber IS NOT NULL)
--UNION ALL
--SELECT dbo.ArchivePacket.PacketID As HeaderID, InvoiceNumber, concat('Your Ref:', ClientPacketNumber) AS ClientRef, SUM(Amount) AS PaymentAmount, dbo.ArchivePacket.PacketNumber,  PacketType,  CustomerVAT, ZeroVatMessage, current_date() as created_at
--FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON dbo.Customer.AccountCode = dbo.ArchivePacket.TradesmanAccountCode
--LEFT OUTER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPAYMENTS') }} ON dbo.ArchivePacket.PacketID = dbo.ArchivePayments.PacketID
--GROUP BY dbo.ArchivePacket.PacketNumber, LeftHall, InvoiceNumber, dbo.ArchivePacket.PacketID, Packed, ClientPacketNumber, WithdrawalDateTime, PacketType, 
--                         CustomerVAT, ZeroVatMessage, SentToSage
--HAVING        (SentToSage IS NOT NULL) AND (WithdrawalDateTime IS NULL) AND (InvoiceNumber IS NOT NULL)
--)

--SELECT * FROM HeaderDim

--{% if is_incremental() %}
  -- Only select records newer than the max created_at in the destination
--  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
--{% endif %}


