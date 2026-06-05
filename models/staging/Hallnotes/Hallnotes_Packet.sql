-- dbt incremental model: hallnotes_packetnumber.sql

{{
    config(
        materialized='incremental',
        unique_key=['PACKETNUMBER', 'RECEIVEDDATE', 'FILE_ID'],
        incremental_strategy='merge'
    )
}}

with source_file as (
    select
        h.FILE_ID,
        h.CREATED_AT,
        h.MODIFIED_AT,
        h._FIVETRAN_FILE_PATH,
        h._FIVETRAN_SYNCED
    from {{ source('sharepoint', 'HALLNOTES') }} h
    where h._FIVETRAN_FILE_PATH like 'root/%'
      and exists (
          select 1
          from directory('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES') d
          where d.RELATIVE_PATH = h._FIVETRAN_FILE_PATH
      )
    {% if is_incremental() %}
      and not exists (
          select 1
          from {{ this }} p
          where p.FILE_ID = h.FILE_ID
      )
    {% endif %}
),

parsed as (
    select
        sf.*,
        AI_PARSE_DOCUMENT(
            TO_FILE('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES', sf._FIVETRAN_FILE_PATH),
            {'mode': 'OCR', 'page_split': true}
        ) as doc
    from source_file sf
),

pages as (
    select
        p.FILE_ID,
        p.CREATED_AT,
        p.MODIFIED_AT,
        p._FIVETRAN_FILE_PATH,
        p._FIVETRAN_SYNCED,
        page.value:content::VARCHAR as page_content,
        page.value:index::INT as page_index,
        MAX(page.value:index::INT) OVER (PARTITION BY p.FILE_ID) as max_page_index
    from parsed p,
    lateral flatten(input => p.doc:pages) page
),

extracted as (
    select
        pg.*,
        SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-llama-3.3-70b',
            CONCAT(
                'Extract the following fields from this document text. Return ONLY a valid JSON object with these exact keys: barcode_value, packet_text, account_code, received_date. If a field is not found, use null.\n\n',
                'Rules:\n',
                '- barcode_value: Look for the MAIN packet/barcode number printed prominently, usually near the top of the page. It matches these patterns: (1) Letter(s) + digits + optional letter suffix like S16582A, S2271XB, N20528, Q53047A (2) Digits + letter suffix like 405599B, 412931C, 412834C (3) Pure 5-7 digit numbers like 412905. The code is 5-10 characters total. IMPORTANT: Preserve the FULL value exactly as printed including ALL leading letters. Do NOT drop or truncate any characters. Do NOT use the "Reg No" value. Do NOT include hallmark quality marks like "A+B", "A+C", "B+", etc. - if you see "A+B 412834C", the packet number is just "412834C". The text may have OCR noise - look carefully even if text is garbled. IMPORTANT: If the page contains "Article Discrepancy Note" text, it is a supplementary page belonging to the PREVIOUS packet - it does NOT start a new packet. The barcode on such pages is the SAME as the preceding packet.\n',
                '- packet_text: Look for a SECOND reference to the packet number in the body text of the page - typically after "Packet No", "Pkt No", "Packet:", in a table row, or in the hallnote header section. This should be read INDEPENDENTLY from barcode_value. If both exist, they should match. If you can only find one instance of the packet number on the page, return null for this field. Do NOT just copy barcode_value - only return a value if you find a genuinely separate occurrence of the packet number in the text.\n',
                '- account_code: Look for a PRINTED/TYPED number specifically after "Account No." or "Acc No." or "Acc No:". Read ALL digits carefully - account codes can be 4 to 6 digits (e.g. 074014, 082536, 51414). Do NOT truncate - read every digit. IGNORE any handwritten account codes. IGNORE any values after "Your Ref" or "Your Ref:" - these are NOT account codes. If the value contains letters or slashes it is NOT an account code - return null. If it appears handwritten return null.\n',
                '- received_date: Look for the RECEIVED date - this is the date at the VERY TOP of the page, usually on the first line, often prefixed with a day of the week (e.g. "Thu 30-Apr-2026"). Do NOT use "Est Comp" dates or estimated completion dates - these appear BELOW the barcode/packet number and are a DIFFERENT field entirely. If the only date you can find is labelled "Est Comp" or "Estimated Completion", return null - there is NO received date. The received date is always at the very start of the document BEFORE the barcode. Also check after "Received:". IMPORTANT: Read the day number carefully - distinguish between similar digits like 7 and 8, 1 and 7. Strip any day-of-week prefix and return in DD-Mon-YYYY format only.\n\n',
                'Document text:\n',
                pg.page_content
            )
        ) as llm_response
    from pages pg
    where pg.page_content is not null
      and LENGTH(pg.page_content) > 50
),

cleaned as (
    select
        e.*,
        TRY_PARSE_JSON(REGEXP_SUBSTR(llm_response, '\\{[^{}]*\\}')) as result
    from extracted e
),

all_detections_raw as (
    select
        UPPER(COALESCE(
            CASE
                WHEN CONTAINS(REPLACE(TRIM(result:barcode_value::VARCHAR), ' ', ''), '+')
                THEN REGEXP_SUBSTR(REPLACE(TRIM(result:barcode_value::VARCHAR), ' ', ''), '[0-9]{4,}[A-Za-z]{0,2}')
                ELSE NULL
            END,
            REGEXP_SUBSTR(REPLACE(TRIM(result:barcode_value::VARCHAR), ' ', ''), '^[A-Za-z]?[0-9]{4,}[A-Za-z]{0,2}$'),
            REGEXP_SUBSTR(REPLACE(TRIM(result:barcode_value::VARCHAR), ' ', ''), '[0-9]{4,}[A-Za-z]{0,2}')
        )) as PACKETNUMBER,
        CASE
            WHEN result:packet_text::VARCHAR IS NOT NULL
                 AND result:packet_text::VARCHAR != 'null'
                 AND REGEXP_LIKE(UPPER(REPLACE(TRIM(result:packet_text::VARCHAR), ' ', '')), '^[A-Za-z]?[0-9]{4,}[A-Za-z]{0,2}$')
                 AND LENGTH(REPLACE(TRIM(result:packet_text::VARCHAR), ' ', '')) BETWEEN 5 AND 10
            THEN UPPER(REPLACE(TRIM(result:packet_text::VARCHAR), ' ', ''))
            ELSE NULL
        END as PACKET_TEXT,
        NULLIF(REPLACE(result:account_code::VARCHAR, ' ', ''), 'null') as ACCOUNTCODE,
        COALESCE(
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD-Mon-YYYY'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'YYYY-MM-DD'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD/MM/YYYY'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD-MM-YYYY')
        ) as RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        page_index,
        max_page_index,
        page_content,
        CASE WHEN CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE') THEN true ELSE false END as is_supplementary
    from cleaned
    where result:barcode_value::VARCHAR is not null
      and result:barcode_value::VARCHAR != 'null'
),

regex_fallback as (
    select
        UPPER(REGEXP_SUBSTR(page_content, '\\b([A-Za-z][0-9]{4,6}[A-Za-z]?)\\b', 1, 1, 'e')) as PACKETNUMBER,
        NULL as PACKET_TEXT,
        NULL as ACCOUNTCODE,
        COALESCE(
            TRY_TO_DATE(REGEXP_SUBSTR(page_content, '(\\d{2}-[A-Za-z]{3}-\\d{4})', 1, 1, 'e'), 'DD-Mon-YYYY'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD-Mon-YYYY')
        ) as RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        page_index,
        max_page_index,
        page_content,
        false as is_supplementary
    from cleaned
    where (result:barcode_value::VARCHAR is null or result:barcode_value::VARCHAR = 'null')
      and NOT CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE')
      and REGEXP_SUBSTR(page_content, '\\b([A-Za-z][0-9]{4,6}[A-Za-z]?)\\b', 1, 1, 'e') is not null
),

all_detections_combined as (
    select * from all_detections_raw
    union all
    select * from regex_fallback
),

all_detections as (
    select
        *,
        case
            when PACKETNUMBER is not null
                 and REGEXP_LIKE(PACKETNUMBER, '^[A-Za-z]?[0-9]+[A-Za-z]{0,2}$')
                 and LENGTH(PACKETNUMBER) between 5 and 10
                 and NOT REGEXP_LIKE(PACKETNUMBER, '^[0-9]{8,}$')
                 and PACKETNUMBER != COALESCE(ACCOUNTCODE, '')
            then true
            else false
        end as is_valid,
        CONTAINS(UPPER(page_content), 'PACKET TYPE:') as is_hallnote_header
    from all_detections_combined
    where PACKETNUMBER is not null
      and LENGTH(PACKETNUMBER) between 5 and 10
      and is_supplementary = false
),

valid_detections_raw as (
    select * from all_detections where is_valid
),

fuzzy_matches as (
    select v.FILE_ID, v.page_index, LEFT(v.PACKETNUMBER, LENGTH(v.PACKETNUMBER) - 1) as resolved_pkt
    from valid_detections_raw v
    where NOT EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
      AND NOT EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER)
      AND EXISTS (SELECT 1 FROM valid_detections_raw sibling WHERE sibling.FILE_ID = v.FILE_ID AND sibling.ACCOUNTCODE = v.ACCOUNTCODE
                  AND LEFT(sibling.PACKETNUMBER, LENGTH(sibling.PACKETNUMBER) - 1) = LEFT(v.PACKETNUMBER, LENGTH(v.PACKETNUMBER) - 1)
                  AND sibling.page_index != v.page_index)
),

valid_detections as (
    select
        COALESCE(
            CASE
                WHEN (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
                      OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER))
                THEN v.PACKETNUMBER
                ELSE NULL
            END,
            CASE
                WHEN v.PACKET_TEXT IS NOT NULL
                     AND v.PACKET_TEXT != v.PACKETNUMBER
                     AND (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKET_TEXT)
                          OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKET_TEXT))
                THEN v.PACKET_TEXT
                ELSE NULL
            END,
            fm.resolved_pkt,
            CASE
                WHEN REGEXP_LIKE(v.PACKETNUMBER, '^[A-Za-z][0-9]+$')
                     AND NOT EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER LIKE LEFT(v.PACKETNUMBER, LENGTH(v.PACKETNUMBER) - 1) || '%' AND p.PACKETNUMBER != SUBSTR(v.PACKETNUMBER, 2))
                     AND NOT EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER LIKE LEFT(v.PACKETNUMBER, LENGTH(v.PACKETNUMBER) - 1) || '%' AND ap.PACKETNUMBER != SUBSTR(v.PACKETNUMBER, 2))
                     AND (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
                          OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2)))
                THEN SUBSTR(v.PACKETNUMBER, 2)
                ELSE NULL
            END,
            v.PACKETNUMBER
        ) as PACKETNUMBER,
        v.ACCOUNTCODE, v.RECEIVEDDATE, v.FILE_ID, v.CREATED_AT, v.MODIFIED_AT, v._FIVETRAN_FILE_PATH, v._FIVETRAN_SYNCED, v.page_index, v.max_page_index, v.is_valid, v.is_hallnote_header
    from valid_detections_raw v
    left join fuzzy_matches fm on fm.FILE_ID = v.FILE_ID and fm.page_index = v.page_index
    where EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER)
       OR (v.PACKET_TEXT IS NOT NULL AND v.PACKET_TEXT != v.PACKETNUMBER
           AND (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKET_TEXT)
                OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKET_TEXT)))
       OR fm.resolved_pkt IS NOT NULL
       OR (REGEXP_LIKE(v.PACKETNUMBER, '^[A-Za-z][0-9]+$')
           AND (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
                OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))))
),

valid_detections_corrected as (
    select
        CASE
            WHEN v.is_hallnote_header
                 AND v.PACKETNUMBER != LEAD(v.PACKETNUMBER) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index)
                 AND v.ACCOUNTCODE = LEAD(v.ACCOUNTCODE) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index)
                 AND NOT COALESCE(LEAD(v.is_hallnote_header) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index), false)
            THEN LEAD(v.PACKETNUMBER) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index)
            ELSE v.PACKETNUMBER
        END as PACKETNUMBER,
        v.ACCOUNTCODE, v.RECEIVEDDATE, v.FILE_ID, v.CREATED_AT, v.MODIFIED_AT, v._FIVETRAN_FILE_PATH, v._FIVETRAN_SYNCED, v.page_index, v.max_page_index, v.is_valid,
        CASE
            WHEN v.is_hallnote_header
                 AND v.PACKETNUMBER != LEAD(v.PACKETNUMBER) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index)
                 AND v.ACCOUNTCODE = LEAD(v.ACCOUNTCODE) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index)
                 AND NOT COALESCE(LEAD(v.is_hallnote_header) OVER (PARTITION BY v.FILE_ID ORDER BY v.page_index), false)
            THEN true
            ELSE false
        END as is_corrected_header
    from valid_detections v
),

valid_ordered as (
    select
        *,
        LAG(PACKETNUMBER) OVER (PARTITION BY FILE_ID ORDER BY page_index) as prev_packet,
        LAG(page_index) OVER (PARTITION BY FILE_ID ORDER BY page_index) as prev_page
    from valid_detections_corrected
    where is_corrected_header = false
),

packet_groups as (
    select
        *,
        SUM(CASE
            WHEN PACKETNUMBER = prev_packet AND page_index = prev_page + 1
            THEN 0 ELSE 1
        END) OVER (PARTITION BY FILE_ID ORDER BY page_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as packet_group
    from valid_ordered
),

valid_deduplicated as (
    select
        PACKETNUMBER,
        MAX_BY(ACCOUNTCODE, CASE WHEN ACCOUNTCODE IS NOT NULL THEN 1 ELSE 0 END) as ACCOUNTCODE,
        MAX_BY(RECEIVEDDATE, CASE WHEN RECEIVEDDATE IS NOT NULL THEN 1 ELSE 0 END) as RECEIVEDDATE,
        FILE_ID,
        MIN(CREATED_AT) as CREATED_AT,
        MIN(MODIFIED_AT) as MODIFIED_AT,
        MIN(_FIVETRAN_FILE_PATH) as _FIVETRAN_FILE_PATH,
        MIN(_FIVETRAN_SYNCED) as _FIVETRAN_SYNCED,
        MIN(page_index) as PAGE_INDEX,
        MAX(max_page_index) as max_page_index,
        1 as rn
    from packet_groups
    group by PACKETNUMBER, FILE_ID, packet_group
),

supplementary_pages as (
    select
        FILE_ID,
        page_index
    from cleaned
    where CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE')
),

supplementary_assigned as (
    select
        vd.PACKETNUMBER,
        vd.FILE_ID,
        sp.page_index
    from supplementary_pages sp
    join (
        select PACKETNUMBER, FILE_ID, PAGE_INDEX as page_index,
               LEAD(PAGE_INDEX) OVER (PARTITION BY FILE_ID ORDER BY PAGE_INDEX) as next_valid_page
        from valid_deduplicated
    ) vd
        on vd.FILE_ID = sp.FILE_ID
        and sp.page_index >= vd.page_index
        and sp.page_index < COALESCE(vd.next_valid_page, sp.page_index + 999)
),

contiguous_pages as (
    select
        PACKETNUMBER,
        FILE_ID,
        page_index,
        page_index - ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER, FILE_ID ORDER BY page_index) as grp
    from (
        select PACKETNUMBER, FILE_ID, page_index
        from valid_detections_corrected
        where is_corrected_header = false
        union all
        select PACKETNUMBER, FILE_ID, page_index
        from supplementary_assigned
    )
),

contiguous_groups as (
    select
        PACKETNUMBER,
        FILE_ID,
        grp,
        MIN(page_index) as first_page,
        MAX(page_index) as last_page,
        ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER, FILE_ID ORDER BY MIN(page_index)) as group_rn
    from contiguous_pages
    group by PACKETNUMBER, FILE_ID, grp
),

first_contiguous_group as (
    select
        PACKETNUMBER,
        FILE_ID,
        first_page,
        last_page as last_page_in_group
    from contiguous_groups
    where group_rn = 1
),

best_valid as (
    select
        vd.PACKETNUMBER,
        vd.ACCOUNTCODE,
        vd.RECEIVEDDATE,
        vd.FILE_ID,
        vd.CREATED_AT,
        vd.MODIFIED_AT,
        vd._FIVETRAN_FILE_PATH,
        vd._FIVETRAN_SYNCED,
        vd.PAGE_INDEX,
        vd.max_page_index,
        fcg.last_page_in_group
    from valid_deduplicated vd
    left join first_contiguous_group fcg
        on fcg.PACKETNUMBER = vd.PACKETNUMBER and fcg.FILE_ID = vd.FILE_ID
    where vd.rn = 1
),

final_pages as (
    select
        bv.*,
        LEAST(
            COALESCE(LEAD(bv.PAGE_INDEX) OVER (PARTITION BY bv.FILE_ID ORDER BY bv.PAGE_INDEX) - 1, bv.max_page_index),
            COALESCE(bv.last_page_in_group, bv.max_page_index)
        ) as PAGE_END
    from best_valid bv
),

enriched as (
    select
        f.PACKETNUMBER,
        COALESCE(pk.ACCOUNTCODE, apk.ACCOUNTCODE, f.ACCOUNTCODE) as ACCOUNTCODE,
        COALESCE(f.RECEIVEDDATE, pk.COUNTERDATE, apk.COUNTERDATE) as RECEIVEDDATE,
        f.FILE_ID,
        f.CREATED_AT,
        f.MODIFIED_AT,
        f._FIVETRAN_FILE_PATH,
        f._FIVETRAN_SYNCED,
        f.PAGE_INDEX,
        f.PAGE_END
    from final_pages f
    left join (
        select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
               ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER ORDER BY COUNTER DESC) as rn
        from {{ source('forge', 'PACKET') }}
    ) pk
        on pk.PACKETNUMBER = f.PACKETNUMBER
        and (pk.COUNTERDATE = f.RECEIVEDDATE or (f.RECEIVEDDATE is null and pk.rn = 1))
    left join (
        select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE,
               ROW_NUMBER() OVER (PARTITION BY PACKETNUMBER ORDER BY COUNTER DESC) as rn
        from {{ source('forge', 'ARCHIVEPACKET') }}
    ) apk
        on apk.PACKETNUMBER = f.PACKETNUMBER
        and (apk.COUNTERDATE = f.RECEIVEDDATE or (f.RECEIVEDDATE is null and apk.rn = 1))
)

select * from enriched e
where not exists (
    select 1
    from {{ source('sharepoint','HALLNOTES_PACKETNUMBER') }} hp
    where hp.PACKETNUMBER = e.PACKETNUMBER
      and hp.RECEIVEDDATE = e.RECEIVEDDATE
)
