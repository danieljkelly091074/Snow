-- dbt model: hallnotes_packet_autocorrect.sql
-- Applies high-confidence automatic corrections to hallnotes_packet
-- Only corrects when there is ONE unambiguous fix available

{{
    config(
        materialized='incremental',
        unique_key=['PACKETNUMBER', 'RECEIVEDDATE', 'FILE_ID'],
        incremental_strategy='delete+insert',
        post_hook=[
            "DELETE FROM {{ ref('hallnotes_packet') }} WHERE (PACKETNUMBER, RECEIVEDDATE, FILE_ID) IN (SELECT original_packetnumber, original_receiveddate, FILE_ID FROM {{ this }} WHERE was_corrected = true)",
            "INSERT INTO {{ ref('hallnotes_packet') }} (PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, PAGE_INDEX, PAGE_END) SELECT PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, PAGE_INDEX, PAGE_END FROM {{ this }} WHERE was_corrected = true"
        ]
    )
}}

-- This model outputs the corrected rows. The post_hooks handle updating the source table.
-- If you prefer not to auto-update, remove the post_hooks and use this as a review table.

with source_packets as (
    select * from {{ ref('hallnotes_packet') }}
    where CREATED_AT > '2026-06-10'::TIMESTAMP
    {% if is_incremental() %}
    and FILE_ID not in (select distinct FILE_ID from {{ this }})
    {% endif %}
),

known_packets as (
    select distinct PACKETNUMBER from {{ source('forge', 'PACKET') }}
    union
    select distinct PACKETNUMBER from {{ source('forge', 'ARCHIVEPACKET') }}
),

forge_lookup as (
    select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
           ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER, COUNTER::DATE ORDER BY COUNTER DESC) as rn
    from {{ source('forge', 'PACKET') }}
    union all
    select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
           ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER, COUNTER::DATE ORDER BY COUNTER DESC) as rn
    from {{ source('forge', 'ARCHIVEPACKET') }}
),

-- Auto-fix 1: Unknown packets with exactly ONE fuzzy match of same length
-- (e.g. E23208 -> E23208C when only one candidate exists after excluding file siblings)
unknown_with_single_match as (
    select
        sp.*,
        kp_match.PACKETNUMBER as corrected_packetnumber
    from source_packets sp
    left join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
    inner join (
        select
            sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER as orig_pkt,
            MIN(kp2.PACKETNUMBER) as PACKETNUMBER
        from source_packets sp2
        left join known_packets kp_check on kp_check.PACKETNUMBER = sp2.PACKETNUMBER
        inner join known_packets kp2
            on kp2.PACKETNUMBER LIKE LEFT(sp2.PACKETNUMBER, LENGTH(sp2.PACKETNUMBER) - 1) || '%'
            and LENGTH(kp2.PACKETNUMBER) = LENGTH(sp2.PACKETNUMBER) + 1
            and kp2.PACKETNUMBER != sp2.PACKETNUMBER
        where kp_check.PACKETNUMBER is null
          and LENGTH(sp2.PACKETNUMBER) >= 5
        group by sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER
        having COUNT(DISTINCT kp2.PACKETNUMBER) = 1
    ) kp_match on kp_match.FILE_ID = sp.FILE_ID and kp_match.PAGE_INDEX = sp.PAGE_INDEX
    where kp.PACKETNUMBER is null
),

-- Auto-fix 2: PAGE_END less than PAGE_INDEX -> set PAGE_END = PAGE_INDEX
invalid_ranges as (
    select sp.*
    from source_packets sp
    where sp.PAGE_END < sp.PAGE_INDEX
),

-- Auto-fix 3: Page overlaps -> cap PAGE_END at next_page_start - 1
overlaps as (
    select
        sp.*,
        LEAD(sp.PAGE_INDEX) OVER (PARTITION BY sp.FILE_ID ORDER BY sp.PAGE_INDEX) - 1 as corrected_page_end
    from source_packets sp
),

overlap_fixes as (
    select o.*
    from overlaps o
    where o.corrected_page_end is not null
      and o.PAGE_END > o.corrected_page_end
),

-- Auto-fix 4: Account code correction from Forge (when packet exists but account is wrong/null)
account_fixes as (
    select
        sp.*,
        fl.ACCOUNTCODE as corrected_accountcode
    from source_packets sp
    inner join forge_lookup fl on fl.PACKETNUMBER = sp.PACKETNUMBER and fl.COUNTERDATE = sp.RECEIVEDDATE and fl.rn = 1
    where (sp.ACCOUNTCODE is null or sp.ACCOUNTCODE != fl.ACCOUNTCODE)
      and fl.ACCOUNTCODE is not null
),

-- Auto-fix 5: Misread packet correction via Forge date + account matching
-- When a packet is NOT in Forge, find Forge packets for the same RECEIVEDDATE
-- that are not already present in the file. Uses account code as a tiebreaker
-- when available. If exactly ONE candidate exists, correct it.
forge_packets_by_date as (
    select distinct PACKETNUMBER, COUNTERDATE, ACCOUNTCODE
    from forge_lookup
    where rn = 1
),

unknown_packets as (
    select sp.*
    from source_packets sp
    left join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
    where kp.PACKETNUMBER is null
),

-- First try: match on RECEIVEDDATE + ACCOUNTCODE (highest confidence)
misread_with_account as (
    select
        up2.FILE_ID, up2.PAGE_INDEX, up2.PACKETNUMBER as orig_pkt,
        MIN(fpd.PACKETNUMBER) as corrected_packetnumber
    from unknown_packets up2
    inner join forge_packets_by_date fpd
        on fpd.COUNTERDATE = up2.RECEIVEDDATE
        and fpd.ACCOUNTCODE = up2.ACCOUNTCODE
    -- Exclude Forge packets already present in this file
    left join source_packets sp_existing
        on sp_existing.FILE_ID = up2.FILE_ID
        and sp_existing.PACKETNUMBER = fpd.PACKETNUMBER
    where sp_existing.PACKETNUMBER is null
      and up2.ACCOUNTCODE is not null
      -- Exclude packets already matched by Fix 1
      and fpd.PACKETNUMBER not in (select corrected_packetnumber from unknown_with_single_match where FILE_ID = up2.FILE_ID)
    group by up2.FILE_ID, up2.PAGE_INDEX, up2.PACKETNUMBER
    having COUNT(DISTINCT fpd.PACKETNUMBER) = 1
),

-- Fallback: match on RECEIVEDDATE only (when account code is null or no account match found)
misread_date_only as (
    select
        up2.FILE_ID, up2.PAGE_INDEX, up2.PACKETNUMBER as orig_pkt,
        MIN(fpd.PACKETNUMBER) as corrected_packetnumber
    from unknown_packets up2
    inner join forge_packets_by_date fpd on fpd.COUNTERDATE = up2.RECEIVEDDATE
    -- Exclude Forge packets already present in this file
    left join source_packets sp_existing
        on sp_existing.FILE_ID = up2.FILE_ID
        and sp_existing.PACKETNUMBER = fpd.PACKETNUMBER
    -- Exclude packets already matched by account-based fix above
    left join misread_with_account mwa
        on mwa.FILE_ID = up2.FILE_ID and mwa.PAGE_INDEX = up2.PAGE_INDEX
    where sp_existing.PACKETNUMBER is null
      and mwa.FILE_ID is null
      -- Exclude packets already matched by Fix 1
      and fpd.PACKETNUMBER not in (select corrected_packetnumber from unknown_with_single_match where FILE_ID = up2.FILE_ID)
    group by up2.FILE_ID, up2.PAGE_INDEX, up2.PACKETNUMBER
    having COUNT(DISTINCT fpd.PACKETNUMBER) = 1
),

misread_fixes as (
    select up.*, match.corrected_packetnumber
    from unknown_packets up
    inner join (
        select FILE_ID, PAGE_INDEX, orig_pkt, corrected_packetnumber from misread_with_account
        union all
        select FILE_ID, PAGE_INDEX, orig_pkt, corrected_packetnumber from misread_date_only
    ) match on match.FILE_ID = up.FILE_ID and match.PAGE_INDEX = up.PAGE_INDEX
),

-- Auto-fix 6: Gap detection — when there's a missing page between rows,
-- look for a Forge packet on the same RECEIVEDDATE not already in the file.
-- If exactly one candidate exists, insert it into the gap.
page_gaps as (
    select
        sp.*,
        LEAD(sp.PAGE_INDEX) OVER (PARTITION BY sp.FILE_ID ORDER BY sp.PAGE_INDEX) as next_page_index,
        LEAD(sp.PAGE_INDEX) OVER (PARTITION BY sp.FILE_ID ORDER BY sp.PAGE_INDEX) - sp.PAGE_END - 1 as gap_size
    from source_packets sp
),

gaps_with_candidates as (
    select
        pg.FILE_ID,
        pg.PAGE_END + 1 as gap_start,
        pg.next_page_index - 1 as gap_end,
        pg.RECEIVEDDATE,
        pg.CREATED_AT,
        pg.MODIFIED_AT,
        pg._FIVETRAN_FILE_PATH,
        pg._FIVETRAN_SYNCED
    from page_gaps pg
    where pg.gap_size > 0
      and pg.next_page_index is not null
),

gap_fixes as (
    select
        g.FILE_ID,
        g.gap_start as PAGE_INDEX,
        g.gap_end as PAGE_END,
        g.CREATED_AT,
        g.MODIFIED_AT,
        g._FIVETRAN_FILE_PATH,
        g._FIVETRAN_SYNCED,
        match.PACKETNUMBER,
        match.ACCOUNTCODE,
        match.RECEIVEDDATE
    from gaps_with_candidates g
    inner join (
        select
            g2.FILE_ID, g2.gap_start,
            MIN(fl2.PACKETNUMBER) as PACKETNUMBER,
            MIN(fl2.ACCOUNTCODE) as ACCOUNTCODE,
            fl2.COUNTERDATE as RECEIVEDDATE
        from gaps_with_candidates g2
        inner join forge_lookup fl2 on fl2.COUNTERDATE = g2.RECEIVEDDATE and fl2.rn = 1
        -- Exclude Forge packets already in this file
        left join source_packets sp_existing
            on sp_existing.FILE_ID = g2.FILE_ID
            and sp_existing.PACKETNUMBER = fl2.PACKETNUMBER
        where sp_existing.PACKETNUMBER is null
        group by g2.FILE_ID, g2.gap_start, fl2.COUNTERDATE
        having COUNT(DISTINCT fl2.PACKETNUMBER) = 1
    ) match on match.FILE_ID = g.FILE_ID and match.gap_start = g.gap_start
),

-- Combine all auto-corrections into final output
all_corrections as (
    -- Fix 1: Unknown packet -> single fuzzy match
    select
        corrected_packetnumber as PACKETNUMBER,
        COALESCE(fl.ACCOUNTCODE, u.ACCOUNTCODE) as ACCOUNTCODE,
        RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        PAGE_INDEX,
        PAGE_END,
        u.PACKETNUMBER as original_packetnumber,
        u.RECEIVEDDATE as original_receiveddate,
        'FUZZY_PACKET_FIX' as correction_type,
        true as was_corrected
    from unknown_with_single_match u
    left join forge_lookup fl on fl.PACKETNUMBER = u.corrected_packetnumber and fl.COUNTERDATE = u.RECEIVEDDATE and fl.rn = 1

    union all

    -- Fix 2: Invalid page range
    select
        PACKETNUMBER,
        ACCOUNTCODE,
        RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        PAGE_INDEX,
        PAGE_INDEX as PAGE_END,
        PACKETNUMBER as original_packetnumber,
        RECEIVEDDATE as original_receiveddate,
        'PAGE_RANGE_FIX' as correction_type,
        true as was_corrected
    from invalid_ranges

    union all

    -- Fix 3: Overlap fix
    select
        PACKETNUMBER,
        ACCOUNTCODE,
        RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        PAGE_INDEX,
        corrected_page_end as PAGE_END,
        PACKETNUMBER as original_packetnumber,
        RECEIVEDDATE as original_receiveddate,
        'OVERLAP_FIX' as correction_type,
        true as was_corrected
    from overlap_fixes

    union all

    -- Fix 4: Account code fix
    select
        PACKETNUMBER,
        corrected_accountcode as ACCOUNTCODE,
        RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        PAGE_INDEX,
        PAGE_END,
        PACKETNUMBER as original_packetnumber,
        RECEIVEDDATE as original_receiveddate,
        'ACCOUNT_FIX' as correction_type,
        true as was_corrected
    from account_fixes

    union all

    -- Fix 5: Misread packet correction (Forge date match with single candidate)
    select
        mf.corrected_packetnumber as PACKETNUMBER,
        COALESCE(fl.ACCOUNTCODE, mf.ACCOUNTCODE) as ACCOUNTCODE,
        mf.RECEIVEDDATE,
        mf.FILE_ID,
        mf.CREATED_AT,
        mf.MODIFIED_AT,
        mf._FIVETRAN_FILE_PATH,
        mf._FIVETRAN_SYNCED,
        mf.PAGE_INDEX,
        mf.PAGE_END,
        mf.PACKETNUMBER as original_packetnumber,
        mf.RECEIVEDDATE as original_receiveddate,
        'MISREAD_PACKET_FIX' as correction_type,
        true as was_corrected
    from misread_fixes mf
    left join forge_lookup fl on fl.PACKETNUMBER = mf.corrected_packetnumber and fl.COUNTERDATE = mf.RECEIVEDDATE and fl.rn = 1

    union all

    -- Fix 6: Gap fill (missing packet inserted from Forge)
    select
        gf.PACKETNUMBER,
        gf.ACCOUNTCODE,
        gf.RECEIVEDDATE,
        gf.FILE_ID,
        gf.CREATED_AT,
        gf.MODIFIED_AT,
        gf._FIVETRAN_FILE_PATH,
        gf._FIVETRAN_SYNCED,
        gf.PAGE_INDEX,
        gf.PAGE_END,
        gf.PACKETNUMBER as original_packetnumber,
        gf.RECEIVEDDATE as original_receiveddate,
        'GAP_FILL_FIX' as correction_type,
        true as was_corrected
    from gap_fixes gf
)

select * from all_corrections
