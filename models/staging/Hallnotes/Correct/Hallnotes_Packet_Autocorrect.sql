-- dbt model: hallnotes_packet_autocorrect.sql
-- Applies high-confidence automatic corrections to hallnotes_packet
-- Only corrects when there is ONE unambiguous fix available

{{
    config(
        materialized='incremental',
        unique_key=['PACKETNUMBER', 'RECEIVEDDATE', 'FILE_ID'],
        incremental_strategy='delete+insert',
        post_hook=[
            "DELETE FROM {{ ref('Hallnotes_Packet') }} WHERE (PACKETNUMBER, RECEIVEDDATE, FILE_ID) IN (SELECT original_packetnumber, original_receiveddate, FILE_ID FROM {{ this }} WHERE was_corrected = true)",
            "INSERT INTO {{ ref('Hallnotes_Packet') }} (PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, PAGE_INDEX, PAGE_END) SELECT PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, PAGE_INDEX, PAGE_END FROM {{ this }} WHERE was_corrected = true"
        ]
    )
}}

-- This model outputs the corrected rows. The post_hooks handle updating the source table.
-- If you prefer not to auto-update, remove the post_hooks and use this as a review table.

with source_packets as (
    select * from {{ ref('Hallnotes_Packet') }}
    where CREATED_AT > '2026-06-10'::TIMESTAMP
    {% if is_incremental() %}
    and FILE_ID not in (select distinct FILE_ID from {{ this }})
    {% endif %}
),

known_packets as (
    select distinct PACKETNUMBER from {{ source('forge', 'PACKET') }}
),

forge_lookup as (
    select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
           ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER, COUNTER::DATE ORDER BY COUNTER DESC) as rn
    from {{ source('forge', 'PACKET') }}
),

-- Auto-fix 1: Unknown packets with exactly ONE fuzzy match of same length
-- (e.g. E23208 -> E23208C when only one candidate exists after excluding file siblings)
unknown_candidates as (
    select
        sp.FILE_ID, sp.PAGE_INDEX, sp.PACKETNUMBER,
        kp2.PACKETNUMBER as candidate_pkt
    from source_packets sp
    left join known_packets kp_check on kp_check.PACKETNUMBER = sp.PACKETNUMBER
    inner join known_packets kp2
        on kp2.PACKETNUMBER LIKE LEFT(sp.PACKETNUMBER, LENGTH(sp.PACKETNUMBER) - 1) || '%'
        and LENGTH(kp2.PACKETNUMBER) = LENGTH(sp.PACKETNUMBER) + 1
        and kp2.PACKETNUMBER != sp.PACKETNUMBER
    where kp_check.PACKETNUMBER is null
      and LENGTH(sp.PACKETNUMBER) >= 5
),

unknown_single_candidates as (
    select FILE_ID, PAGE_INDEX, PACKETNUMBER as orig_pkt, MIN(candidate_pkt) as PACKETNUMBER
    from unknown_candidates
    group by FILE_ID, PAGE_INDEX, PACKETNUMBER
    having COUNT(DISTINCT candidate_pkt) = 1
),

unknown_with_single_match as (
    select
        sp.*,
        usc.PACKETNUMBER as corrected_packetnumber
    from source_packets sp
    inner join unknown_single_candidates usc on usc.FILE_ID = sp.FILE_ID and usc.PAGE_INDEX = sp.PAGE_INDEX
    left join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
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
    -- Packets not in Forge at all
    select sp.*
    from source_packets sp
    left join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
    where kp.PACKETNUMBER is null
),

-- Packets that exist in Forge but have NO matching record for their account+date combo
-- These are likely misreads where the OCR coincidentally produced a real packet number
known_but_mismatched as (
    select sp.*
    from source_packets sp
    inner join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
    where sp.ACCOUNTCODE is not null
      and not exists (
          select 1 from forge_lookup fl
          where fl.PACKETNUMBER = sp.PACKETNUMBER
            and fl.COUNTERDATE = sp.RECEIVEDDATE
            and fl.ACCOUNTCODE = sp.ACCOUNTCODE
      )
),

-- Combine both types as candidates for Fix 5
suspect_packets as (
    select * from unknown_packets
    union all
    select * from known_but_mismatched
),

-- First try: match on RECEIVEDDATE + ACCOUNTCODE (highest confidence)
misread_with_account as (
    select
        sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER as orig_pkt,
        MIN(fpd.PACKETNUMBER) as corrected_packetnumber
    from suspect_packets sp2
    inner join forge_packets_by_date fpd
        on fpd.COUNTERDATE = sp2.RECEIVEDDATE
        and fpd.ACCOUNTCODE = sp2.ACCOUNTCODE
    -- Exclude Forge packets already present in this file
    left join source_packets sp_existing
        on sp_existing.FILE_ID = sp2.FILE_ID
        and sp_existing.PACKETNUMBER = fpd.PACKETNUMBER
    where sp_existing.PACKETNUMBER is null
      and sp2.ACCOUNTCODE is not null
      -- Exclude packets already matched by Fix 1
      and fpd.PACKETNUMBER not in (select corrected_packetnumber from unknown_with_single_match where FILE_ID = sp2.FILE_ID)
      -- Don't correct to the same packet number
      and fpd.PACKETNUMBER != sp2.PACKETNUMBER
    group by sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER
    having COUNT(DISTINCT fpd.PACKETNUMBER) = 1
),

-- Suffix tiebreaker: when misread_with_account found multiple candidates (blocked by HAVING = 1),
-- but the misread packet ends in a letter suffix and exactly one candidate shares that suffix,
-- prefer that candidate. E.g. S16582A -> 416307A (not 416307B) because both end in "A".
misread_suffix_tiebreaker as (
    select
        sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER as orig_pkt,
        MIN(fpd.PACKETNUMBER) as corrected_packetnumber
    from suspect_packets sp2
    inner join forge_packets_by_date fpd
        on fpd.COUNTERDATE = sp2.RECEIVEDDATE
        and fpd.ACCOUNTCODE = sp2.ACCOUNTCODE
    -- Exclude Forge packets already present in this file
    left join source_packets sp_existing
        on sp_existing.FILE_ID = sp2.FILE_ID
        and sp_existing.PACKETNUMBER = fpd.PACKETNUMBER
    -- Exclude those already resolved by single-candidate match
    left join misread_with_account mwa
        on mwa.FILE_ID = sp2.FILE_ID and mwa.PAGE_INDEX = sp2.PAGE_INDEX
    where sp_existing.PACKETNUMBER is null
      and sp2.ACCOUNTCODE is not null
      and mwa.FILE_ID is null
      and fpd.PACKETNUMBER not in (select corrected_packetnumber from unknown_with_single_match where FILE_ID = sp2.FILE_ID)
      and fpd.PACKETNUMBER != sp2.PACKETNUMBER
      -- Suffix match: misread packet ends in a letter, and candidate ends in the same letter
      and REGEXP_LIKE(sp2.PACKETNUMBER, '.*[A-Za-z]$')
      and RIGHT(fpd.PACKETNUMBER, 1) = RIGHT(sp2.PACKETNUMBER, 1)
    group by sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER
    having COUNT(DISTINCT fpd.PACKETNUMBER) = 1
),

-- Fallback: match on RECEIVEDDATE only (when account code is null or no account match found)
misread_date_only as (
    select
        sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER as orig_pkt,
        MIN(fpd.PACKETNUMBER) as corrected_packetnumber
    from suspect_packets sp2
    inner join forge_packets_by_date fpd on fpd.COUNTERDATE = sp2.RECEIVEDDATE
    -- Exclude Forge packets already present in this file
    left join source_packets sp_existing
        on sp_existing.FILE_ID = sp2.FILE_ID
        and sp_existing.PACKETNUMBER = fpd.PACKETNUMBER
    -- Exclude packets already matched by account-based fix or suffix tiebreaker
    left join misread_with_account mwa
        on mwa.FILE_ID = sp2.FILE_ID and mwa.PAGE_INDEX = sp2.PAGE_INDEX
    left join misread_suffix_tiebreaker mst
        on mst.FILE_ID = sp2.FILE_ID and mst.PAGE_INDEX = sp2.PAGE_INDEX
    where sp_existing.PACKETNUMBER is null
      and mwa.FILE_ID is null
      and mst.FILE_ID is null
      -- Exclude packets already matched by Fix 1
      and fpd.PACKETNUMBER not in (select corrected_packetnumber from unknown_with_single_match where FILE_ID = sp2.FILE_ID)
      -- Don't correct to the same packet number
      and fpd.PACKETNUMBER != sp2.PACKETNUMBER
    group by sp2.FILE_ID, sp2.PAGE_INDEX, sp2.PACKETNUMBER
    having COUNT(DISTINCT fpd.PACKETNUMBER) = 1
),

misread_fixes as (
    select sp2.*, match.corrected_packetnumber
    from suspect_packets sp2
    inner join (
        select FILE_ID, PAGE_INDEX, orig_pkt, corrected_packetnumber from misread_with_account
        union all
        select FILE_ID, PAGE_INDEX, orig_pkt, corrected_packetnumber from misread_suffix_tiebreaker
        union all
        select FILE_ID, PAGE_INDEX, orig_pkt, corrected_packetnumber from misread_date_only
    ) match on match.FILE_ID = sp2.FILE_ID and match.PAGE_INDEX = sp2.PAGE_INDEX
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

-- Auto-fix 7: Sibling detection — when a packet exists (or was corrected by Fix 5) and Forge has
-- siblings (same base, different suffix e.g. 416307A/416307B) for the same date that are
-- missing from the file, AND an adjacent row has PAGE_END > PAGE_INDEX (inflated range),
-- trim that row and insert the missing sibling into the freed page(s).
-- Includes packets corrected by Fix 5 in the same run so both fixes can chain together.
all_known_in_file as (
    -- Existing known packets
    select sp.FILE_ID, sp.PACKETNUMBER, sp.RECEIVEDDATE, sp.PAGE_INDEX, sp.PAGE_END,
           sp.CREATED_AT, sp.MODIFIED_AT, sp._FIVETRAN_FILE_PATH, sp._FIVETRAN_SYNCED
    from source_packets sp
    inner join known_packets kp on kp.PACKETNUMBER = sp.PACKETNUMBER
    union all
    -- Packets that Fix 5 will correct in this run (e.g. S16582A -> 416307A)
    select mf.FILE_ID, mf.corrected_packetnumber as PACKETNUMBER, mf.RECEIVEDDATE,
           mf.PAGE_INDEX, mf.PAGE_END,
           mf.CREATED_AT, mf.MODIFIED_AT, mf._FIVETRAN_FILE_PATH, mf._FIVETRAN_SYNCED
    from misread_fixes mf
),

existing_with_siblings as (
    select
        akf.FILE_ID, akf.PACKETNUMBER, akf.RECEIVEDDATE, akf.PAGE_INDEX, akf.PAGE_END,
        akf.CREATED_AT, akf.MODIFIED_AT, akf._FIVETRAN_FILE_PATH, akf._FIVETRAN_SYNCED,
        LEFT(akf.PACKETNUMBER, LENGTH(akf.PACKETNUMBER) - 1) as base_pkt
    from all_known_in_file akf
    where REGEXP_LIKE(akf.PACKETNUMBER, '.*[A-Za-z]$')
),

missing_siblings as (
    select
        es.FILE_ID, es.PACKETNUMBER as existing_pkt, es.RECEIVEDDATE,
        es.PAGE_INDEX as existing_page_index, es.PAGE_END as existing_page_end,
        es.CREATED_AT, es.MODIFIED_AT, es._FIVETRAN_FILE_PATH, es._FIVETRAN_SYNCED,
        fl.PACKETNUMBER as sibling_pkt,
        fl.ACCOUNTCODE as sibling_accountcode
    from existing_with_siblings es
    inner join forge_lookup fl
        on LEFT(fl.PACKETNUMBER, LENGTH(fl.PACKETNUMBER) - 1) = es.base_pkt
        and fl.PACKETNUMBER != es.PACKETNUMBER
        and fl.COUNTERDATE = es.RECEIVEDDATE
        and fl.rn = 1
    -- Sibling must not already be in the file (including Fix 5 corrections)
    left join all_known_in_file akf_check
        on akf_check.FILE_ID = es.FILE_ID
        and akf_check.PACKETNUMBER = fl.PACKETNUMBER
    where akf_check.PACKETNUMBER is null
),

-- Find adjacent rows with inflated PAGE_END that could contain the missing sibling
sibling_with_donor as (
    select
        ms.*,
        donor.PACKETNUMBER as donor_pkt,
        donor.PAGE_INDEX as donor_page_index,
        donor.PAGE_END as donor_page_end,
        donor.ACCOUNTCODE as donor_accountcode,
        donor.RECEIVEDDATE as donor_receiveddate
    from missing_siblings ms
    -- Look for the row immediately after or before the existing packet that spans multiple pages
    inner join source_packets donor
        on donor.FILE_ID = ms.FILE_ID
        and donor.PAGE_END > donor.PAGE_INDEX
        and donor.PACKETNUMBER != ms.existing_pkt
        -- Donor is adjacent: either immediately before or after the existing packet
        and (donor.PAGE_END = ms.existing_page_index - 1 OR donor.PAGE_INDEX = ms.existing_page_end + 1)
),

sibling_fixes as (
    select
        sd.FILE_ID,
        sd.sibling_pkt as PACKETNUMBER,
        sd.sibling_accountcode as ACCOUNTCODE,
        sd.RECEIVEDDATE,
        sd.CREATED_AT,
        sd.MODIFIED_AT,
        sd._FIVETRAN_FILE_PATH,
        sd._FIVETRAN_SYNCED,
        sd.existing_page_end,
        -- Allocate the last page of the donor to the sibling
        CASE
            WHEN sd.donor_page_index = sd.existing_page_end + 1
            THEN sd.donor_page_end  -- sibling gets last page of donor after existing
            ELSE sd.donor_page_index  -- sibling gets first page of donor before existing
        END as PAGE_INDEX,
        CASE
            WHEN sd.donor_page_index = sd.existing_page_end + 1
            THEN sd.donor_page_end
            ELSE sd.donor_page_index
        END as PAGE_END,
        sd.donor_pkt as donor_packetnumber,
        sd.donor_page_index,
        sd.donor_page_end
    from sibling_with_donor sd
),

-- Also emit the trimmed donor row
sibling_donor_trims as (
    select
        sf.FILE_ID,
        sf.donor_packetnumber as PACKETNUMBER,
        sp_donor.ACCOUNTCODE,
        sp_donor.RECEIVEDDATE,
        sp_donor.CREATED_AT,
        sp_donor.MODIFIED_AT,
        sp_donor._FIVETRAN_FILE_PATH,
        sp_donor._FIVETRAN_SYNCED,
        -- Trim the donor: adjust PAGE_INDEX or PAGE_END depending on which page was taken
        CASE
            WHEN sf.donor_page_index = sf.existing_page_end + 1
            THEN sf.donor_page_index  -- donor after existing: keep start, trim end
            ELSE sf.donor_page_index + 1  -- donor before existing: shift start forward
        END as PAGE_INDEX,
        CASE
            WHEN sf.donor_page_index = sf.existing_page_end + 1
            THEN sf.donor_page_end - 1  -- donor after existing: trim last page
            ELSE sf.donor_page_end  -- donor before existing: keep end
        END as PAGE_END
    from sibling_fixes sf
    inner join source_packets sp_donor
        on sp_donor.FILE_ID = sf.FILE_ID
        and sp_donor.PACKETNUMBER = sf.donor_packetnumber
        and sp_donor.PAGE_INDEX = sf.donor_page_index
    -- Only trim if donor still has at least 1 page left
    where sf.donor_page_end - sf.donor_page_index >= 1
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

    union all

    -- Fix 7a: Insert missing sibling packet
    select
        sf.PACKETNUMBER,
        sf.ACCOUNTCODE,
        sf.RECEIVEDDATE,
        sf.FILE_ID,
        sf.CREATED_AT,
        sf.MODIFIED_AT,
        sf._FIVETRAN_FILE_PATH,
        sf._FIVETRAN_SYNCED,
        sf.PAGE_INDEX,
        sf.PAGE_END,
        sf.donor_packetnumber as original_packetnumber,
        sf.RECEIVEDDATE as original_receiveddate,
        'SIBLING_INSERT_FIX' as correction_type,
        true as was_corrected
    from sibling_fixes sf

    union all

    -- Fix 7b: Trim donor row that gave up a page to the sibling
    select
        sdt.PACKETNUMBER,
        sdt.ACCOUNTCODE,
        sdt.RECEIVEDDATE,
        sdt.FILE_ID,
        sdt.CREATED_AT,
        sdt.MODIFIED_AT,
        sdt._FIVETRAN_FILE_PATH,
        sdt._FIVETRAN_SYNCED,
        sdt.PAGE_INDEX,
        sdt.PAGE_END,
        sdt.PACKETNUMBER as original_packetnumber,
        sdt.RECEIVEDDATE as original_receiveddate,
        'SIBLING_TRIM_FIX' as correction_type,
        true as was_corrected
    from sibling_donor_trims sdt
)

select * from all_corrections
