-- dbt model: hallnotes_packet_validation.sql
-- Flags anomalies in hallnotes_packet for review

{{
    config(
        materialized='table'
    )
}}

with packets as (
    select *,
        LEAD(PAGE_INDEX) OVER (PARTITION BY FILE_ID ORDER BY PAGE_INDEX) as next_page_index,
        LAG(PAGE_END) OVER (PARTITION BY FILE_ID ORDER BY PAGE_INDEX) as prev_page_end
    from {{ ref('hallnotes_packetnumber') }}
),

known_packets as (
    select distinct PACKETNUMBER from {{ source('forge', 'PACKET') }}
    union
    select distinct PACKETNUMBER from {{ source('forge', 'ARCHIVEPACKET') }}
),

forge_lookup as (
    select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
           ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER ORDER BY COUNTER DESC) as rn
    from {{ source('forge', 'PACKET') }}
    union all
    select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
           ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER ORDER BY COUNTER DESC) as rn
    from {{ source('forge', 'ARCHIVEPACKET') }}
),

-- Check 1: Unknown packets (not in PACKET or ARCHIVEPACKET)
unknown_packets as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'UNKNOWN_PACKET' as issue_type,
        'Packet number not found in PACKET or ARCHIVEPACKET' as issue_description,
        NULL as suggested_fix
    from packets p
    left join known_packets kp on kp.PACKETNUMBER = p.PACKETNUMBER
    where kp.PACKETNUMBER is null
),

-- Check 2: Page gaps (missing pages between consecutive packets)
page_gaps as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'PAGE_GAP' as issue_type,
        'Gap of ' || (p.next_page_index - p.PAGE_END - 1) || ' page(s) between this packet (end ' || p.PAGE_END || ') and next (start ' || p.next_page_index || ')' as issue_description,
        'Possible missed packet between pages ' || (p.PAGE_END + 1) || ' and ' || (p.next_page_index - 1) as suggested_fix
    from packets p
    where p.next_page_index is not null
      and p.next_page_index - p.PAGE_END > 2
),

-- Check 3: Page overlaps (PAGE_END >= next PAGE_INDEX)
page_overlaps as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'PAGE_OVERLAP' as issue_type,
        'PAGE_END (' || p.PAGE_END || ') overlaps with next packet starting at page ' || p.next_page_index as issue_description,
        'Set PAGE_END to ' || (p.next_page_index - 1) as suggested_fix
    from packets p
    where p.next_page_index is not null
      and p.PAGE_END >= p.next_page_index
),

-- Check 4: PAGE_END less than PAGE_INDEX
invalid_range as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'INVALID_PAGE_RANGE' as issue_type,
        'PAGE_END (' || p.PAGE_END || ') is less than PAGE_INDEX (' || p.PAGE_INDEX || ')' as issue_description,
        'Set PAGE_END to ' || p.PAGE_INDEX as suggested_fix
    from packets p
    where p.PAGE_END < p.PAGE_INDEX
),

-- Check 5: Account code mismatch with Forge
account_mismatches as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'ACCOUNT_MISMATCH' as issue_type,
        'Detected account ' || COALESCE(p.ACCOUNTCODE, 'NULL') || ' but Forge shows ' || fl.ACCOUNTCODE as issue_description,
        'Update ACCOUNTCODE to ' || fl.ACCOUNTCODE as suggested_fix
    from packets p
    inner join forge_lookup fl on fl.PACKETNUMBER = p.PACKETNUMBER and fl.COUNTERDATE = p.RECEIVEDDATE
    where p.ACCOUNTCODE is not null
      and fl.ACCOUNTCODE is not null
      and p.ACCOUNTCODE != fl.ACCOUNTCODE
),

-- Check 6: Received date mismatch (no matching counter date in Forge)
date_mismatches as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'DATE_MISMATCH' as issue_type,
        'Received date ' || p.RECEIVEDDATE || ' has no matching counter date in Forge' as issue_description,
        'Check if date is misread - nearest Forge dates: ' || 
            COALESCE((select LISTAGG(DISTINCT fl2.COUNTERDATE::VARCHAR, ', ') WITHIN GROUP (ORDER BY fl2.COUNTERDATE DESC)
             from forge_lookup fl2 where fl2.PACKETNUMBER = p.PACKETNUMBER and fl2.COUNTERDATE between p.RECEIVEDDATE - 7 and p.RECEIVEDDATE + 7), 'none found') as suggested_fix
    from packets p
    inner join known_packets kp on kp.PACKETNUMBER = p.PACKETNUMBER
    where p.RECEIVEDDATE is not null
      and not exists (
          select 1 from forge_lookup fl where fl.PACKETNUMBER = p.PACKETNUMBER and fl.COUNTERDATE = p.RECEIVEDDATE
      )
),

-- Check 7: Duplicate packets in same file
duplicate_packets as (
    select
        p.FILE_ID, p.PACKETNUMBER, p.ACCOUNTCODE, p.RECEIVEDDATE, p.PAGE_INDEX, p.PAGE_END,
        'DUPLICATE_IN_FILE' as issue_type,
        'Packet appears ' || cnt.pkt_count || ' times in same file' as issue_description,
        'Review if duplicate detection or legitimate multi-instance' as suggested_fix
    from packets p
    inner join (
        select FILE_ID, PACKETNUMBER, COUNT(*) as pkt_count
        from {{ ref('hallnotes_packetnumber') }}
        group by FILE_ID, PACKETNUMBER
        having COUNT(*) > 1
    ) cnt on cnt.FILE_ID = p.FILE_ID and cnt.PACKETNUMBER = p.PACKETNUMBER
),

-- Check 8: Fuzzy match candidates (unknown packet with close match in Forge)
fuzzy_candidates as (
    select
        up.FILE_ID, up.PACKETNUMBER, up.ACCOUNTCODE, up.RECEIVEDDATE, up.PAGE_INDEX, up.PAGE_END,
        'FUZZY_MATCH_AVAILABLE' as issue_type,
        'Unknown packet "' || up.PACKETNUMBER || '" has close match: ' || kp.PACKETNUMBER as issue_description,
        'UPDATE SET PACKETNUMBER = ''' || kp.PACKETNUMBER || '''' as suggested_fix
    from unknown_packets up
    inner join known_packets kp
        on kp.PACKETNUMBER LIKE LEFT(up.PACKETNUMBER, LENGTH(up.PACKETNUMBER) - 1) || '%'
        and LENGTH(kp.PACKETNUMBER) = LENGTH(up.PACKETNUMBER)
        and kp.PACKETNUMBER != up.PACKETNUMBER
)

select * from unknown_packets
union all select * from page_gaps
union all select * from page_overlaps
union all select * from invalid_range
union all select * from account_mismatches
union all select * from date_mismatches
union all select * from duplicate_packets
union all select * from fuzzy_candidates
order by FILE_ID, PAGE_INDEX, issue_type
