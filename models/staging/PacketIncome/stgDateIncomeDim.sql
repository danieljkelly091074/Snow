{{ config(materilized='table')}}

WITH DateIncome As (
SELECT DISTINCT TO_CHAR(dbo.Packet.Opened, 'YYYYMMDD')::INT AS DATEKEY, TO_DATE(TO_VARCHAR(dbo.Packet.Opened, 'DD/MM/YYYY'),'DD/MM/YYYY') As OpenedDate, DATE_PART(Day, dbo.Packet.Opened) AS Day, DATE_PART(Month, dbo.Packet.Opened) AS Month, DATE_PART(Year, dbo.Packet.Opened) AS Year, DATE_PART(Quarter, dbo.Packet.Opened) AS Quarter, MonthName(TO_DATE(dbo.Packet.Opened)) AS MonthName, DAYNAME(TO_DATE(dbo.Packet.Opened)) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'PACKET') }}
WHERE       dbo.Packet.Opened>= '01-01-2015 00:00:00' AND dbo.Packet.Opened IS NOT NULL
UNION
SELECT DISTINCT TO_CHAR(dbo.ArchivePacket.Opened, 'YYYYMMDD')::INT AS DATEKEY, TO_DATE(TO_VARCHAR(dbo.ArchivePacket.Opened, 'DD/MM/YYYY'),'DD/MM/YYYY') As OpenedDate, DATE_PART(Day, dbo.ArchivePacket.Opened) AS Day, DATE_PART(Month, dbo.ArchivePacket.Opened) AS Month, DATE_PART(Year, dbo.ArchivePacket.Opened) AS Year, DATE_PART(Quarter, dbo.ArchivePacket.Opened) AS Quarter, MonthName(TO_DATE(dbo.ArchivePacket.Opened)) AS MonthName, DAYNAME(TO_DATE(dbo.ArchivePacket.Opened)) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }}
WHERE       dbo.ArchivePacket.Opened>= '01-01-2015 00:00:00' and dbo.ArchivePacket.Opened IS NOT NULL
)

SELECT * FROM DateIncome