{{ config(materialized="table") }}

with
    dateincome as (
        select distinct
            to_char(dbo.packet.opened, 'YYYYMMDD')::int as datekey,
            to_date(
                to_varchar(dbo.packet.opened, 'DD/MM/YYYY'), 'DD/MM/YYYY'
            ) as openeddate,
            date_part(day, dbo.packet.opened) as day,
            date_part(month, dbo.packet.opened) as month,
            date_part(year, dbo.packet.opened) as year,
            date_part(quarter, dbo.packet.opened) as quarter,
            monthname(to_date(dbo.packet.opened)) as monthname,
            dayname(to_date(dbo.packet.opened)) as dayname
        from {{ source("DBT_SNOWFLAKE", "PACKET") }}
        where
            dbo.PACKET.COUNTER >= TO_TIMESTAMP('01-01-2015 00:00:00', 'DD-MM-YYYY HH24:MI:SS') and dbo.PACKET.COUNTER is not null
        union
        select distinct
            to_char(dbo.archivepacket.opened, 'YYYYMMDD')::int as datekey,
            to_date(
                to_varchar(dbo.archivepacket.opened, 'DD/MM/YYYY'), 'DD/MM/YYYY'
            ) as openeddate,
            date_part(day, dbo.archivepacket.opened) as day,
            date_part(month, dbo.archivepacket.opened) as month,
            date_part(year, dbo.archivepacket.opened) as year,
            date_part(quarter, dbo.archivepacket.opened) as quarter,
            monthname(to_date(dbo.archivepacket.opened)) as monthname,
            dayname(to_date(dbo.archivepacket.opened)) as dayname
        from {{ source("DBT_SNOWFLAKE", "ARCHIVEPACKET") }}
        where
            dbo.ARCHIVEPACKET.COUNTER  >= TO_TIMESTAMP('01-01-2015 00:00:00', 'DD-MM-YYYY HH24:MI:SS')
            and dbo.ARCHIVEPACKET.COUNTER is not null
    )

select *
from dateincome
