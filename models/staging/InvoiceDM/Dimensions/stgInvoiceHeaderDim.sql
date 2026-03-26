{{ config(
    materialized='incremental',
    unique_key='HEADERID'
) }}

WITH HeaderDim AS (

SELECT Packet.PacketID AS HeaderID, InvoiceNumber, concat('Your Ref:', ClientPacketNumber) AS ClientRef, SUM(Amount) AS PaymentAmount, Packet.PacketNumber, PacketType, CustomerVAT, ZeroVatMessage, current_date() AS created_at
FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKET') }} ON Customer.AccountCode = Packet.TradesmanAccountCode
LEFT OUTER JOIN {{ source('DBT_SNOWFLAKE', 'PAYMENTS') }} ON Packet.PacketNumber = Payments.PacketNumber
GROUP BY Packet.PacketNumber, LeftHall, InvoiceNumber, Packet.PacketID, Packed, ClientPacketNumber, WithdrawalDateTime, PacketType,
                         CustomerVAT, ZeroVatMessage, SentToSage
HAVING        (SentToSage IS NOT NULL) AND (WithdrawalDateTime IS NULL) AND (InvoiceNumber IS NOT NULL)
UNION ALL
SELECT ArchivePacket.PacketID AS HeaderID, InvoiceNumber, concat('Your Ref:', ClientPacketNumber) AS ClientRef, SUM(Amount) AS PaymentAmount, ArchivePacket.PacketNumber, PacketType, CustomerVAT, ZeroVatMessage, current_date() AS created_at
FROM {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }} INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ON Customer.AccountCode = ArchivePacket.TradesmanAccountCode
LEFT OUTER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPAYMENTS') }} ON ArchivePacket.PacketID = ArchivePayments.PacketID
GROUP BY ArchivePacket.PacketNumber, LeftHall, InvoiceNumber, ArchivePacket.PacketID, Packed, ClientPacketNumber, WithdrawalDateTime, PacketType,
                         CustomerVAT, ZeroVatMessage, SentToSage
HAVING        (SentToSage IS NOT NULL) AND (WithdrawalDateTime IS NULL) AND (InvoiceNumber IS NOT NULL)
)

SELECT * FROM HeaderDim

{% if is_incremental() %}
WHERE HeaderID NOT IN (SELECT HeaderID FROM {{ this }})
{% endif %}

