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

-- Combine all auto-corrections into final output
all_corrections as (
    -- Fix 1: Unknown packet -> single fuzzy match
    select
        corrected_packetnumber as PACKETNUMBER,
        COALESCE(fl.ACCOUNTCODE, ACCOUNTCODE) as ACCOUNTCODE,
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
)

select * from all_corrections