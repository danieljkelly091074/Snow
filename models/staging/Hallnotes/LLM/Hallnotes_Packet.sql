-- dbt model: Hallnotes_Packet_v2.sql
-- Simplified hallnotes packet extraction: OCR -> LLM -> one packet per page -> group -> enrich
-- Trusts the LLM's primary barcode_value detection. Autocorrect model handles corrections.

{{
    config(
        materialized='incremental',
        unique_key=['PACKETNUMBER', 'RECEIVEDDATE', 'FILE_ID'],
        incremental_strategy='delete+insert'
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

-- Step 1: OCR the PDF with page splitting
parsed as (
    select
        sf.*,
        AI_PARSE_DOCUMENT(
            TO_FILE('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES', sf._FIVETRAN_FILE_PATH),
            {'mode': 'OCR', 'page_split': true}
        ) as doc
    from source_file sf
),

-- Step 2: Flatten to one row per page
pages as (
    select
        p.FILE_ID,
        p.CREATED_AT,
        p.MODIFIED_AT,
        p._FIVETRAN_FILE_PATH,
        p._FIVETRAN_SYNCED,
        page.value:content::VARCHAR as page_content,
        page.value:index::INT as page_index
    from parsed p,
    lateral flatten(input => p.doc:pages) page
),

-- Step 3: LLM extraction - one call per page
extracted as (
    select
        pg.*,
        AI_COMPLETE(
            'snowflake-llama-3.3-70b',
            CONCAT(
                'Extract the following fields from this document text. If a field is not found, use null.\n\n',
                'Rules:\n',
                '- barcode_value: The MAIN packet/barcode number printed prominently near the top. ',
                'Patterns: Letter(s)+digits+optional suffix (S16582A, N20528, Q53047A), ',
                'Digits+suffix (405599B, 412931C), or pure digits (412905). 5-10 chars total. ',
                'Preserve FULL value exactly as printed. Do NOT use "Reg No". ',
                'Ignore hallmark marks like "A+B". ',
                'If page has "Article Discrepancy Note" it belongs to the PREVIOUS packet - return null.\n',
                '- account_code: PRINTED number after "Account No." or "Acc No." or "Acc No:". ',
                '4-6 digits only. Ignore handwritten codes. Ignore "Your Ref:" values.\n',
                '- received_date: Date at the VERY TOP of page, before the barcode. ',
                'Often prefixed with day of week (e.g. "Thu 30-Apr-2026"). ',
                'Do NOT use "Est Comp" dates. Return in DD-Mon-YYYY format.\n\n',
                'Document text:\n',
                pg.page_content
            ),
            response_format => {'type': 'json', 'schema': {
                'type': 'object',
                'properties': {
                    'barcode_value': {'type': 'string'},
                    'account_code': {'type': 'string'},
                    'received_date': {'type': 'string'}
                }
            }}
        ) as llm_response
    from pages pg
    where pg.page_content is not null
      and LENGTH(pg.page_content) > 50
),

-- Step 4: Parse LLM response into structured fields
cleaned as (
    select
        e.FILE_ID,
        e.CREATED_AT,
        e.MODIFIED_AT,
        e._FIVETRAN_FILE_PATH,
        e._FIVETRAN_SYNCED,
        e.page_index,
        e.page_content,
        TRY_PARSE_JSON(e.llm_response) as result
    from extracted e
),

-- Step 5: One detection per page - trust the LLM's barcode_value
detections as (
    select
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        page_index,
        -- Clean the packet number
        UPPER(REPLACE(TRIM(result:barcode_value::VARCHAR), ' ', '')) as raw_packetnumber,
        -- Account code: LLM first, regex fallback
        COALESCE(
            NULLIF(REPLACE(result:account_code::VARCHAR, ' ', ''), 'null'),
            REGEXP_SUBSTR(page_content, 'Acc\\s*No[.:\\s]*(\\d{4,6})', 1, 1, 'ie', 1)
        ) as ACCOUNTCODE,
        -- Received date with multiple format attempts
        COALESCE(
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD-Mon-YYYY'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'YYYY-MM-DD'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD/MM/YYYY'),
            TRY_TO_DATE(result:received_date::VARCHAR, 'DD-MM-YYYY')
        ) as RECEIVEDDATE,
        -- Flag supplementary pages
        CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE') as is_supplementary
    from cleaned
    where result:barcode_value::VARCHAR is not null
      and result:barcode_value::VARCHAR != 'null'
),

-- Step 6: Validate and normalize packet numbers
valid_detections as (
    select
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        page_index,
        -- Extract valid packet number pattern from raw value
        COALESCE(
            REGEXP_SUBSTR(raw_packetnumber, '^[A-Z]?[0-9]{4,}[A-Z]{0,2}$'),
            REGEXP_SUBSTR(raw_packetnumber, '[0-9]{4,}[A-Z]{0,2}')
        ) as PACKETNUMBER,
        ACCOUNTCODE,
        RECEIVEDDATE,
        is_supplementary
    from detections
    where NOT is_supplementary
      and LENGTH(raw_packetnumber) between 5 and 10
),

-- Step 7: Remove pages with no valid detection
pages_with_packets as (
    select *
    from valid_detections
    where PACKETNUMBER is not null
      and REGEXP_LIKE(PACKETNUMBER, '^[A-Z]?[0-9]+[A-Z]{0,2}$')
      and LENGTH(PACKETNUMBER) between 5 and 10
),

-- Step 8: Assign supplementary pages to preceding packet
supplementary_pages as (
    select
        c.FILE_ID,
        c.page_index
    from cleaned c
    where CONTAINS(UPPER(c.page_content), 'ARTICLE DISCREPANCY NOTE')
),

-- Step 9: Determine page ranges
-- Each packet gets PAGE_INDEX = its first page, PAGE_END = last page before next packet starts
-- Supplementary pages extend the preceding packet's range
all_pages_ordered as (
    select
        p.FILE_ID,
        p.page_index,
        p.PACKETNUMBER,
        p.ACCOUNTCODE,
        p.RECEIVEDDATE,
        p.CREATED_AT,
        p.MODIFIED_AT,
        p._FIVETRAN_FILE_PATH,
        p._FIVETRAN_SYNCED,
        LAG(p.PACKETNUMBER) OVER (PARTITION BY p.FILE_ID ORDER BY p.page_index) as prev_packet,
        LAG(p.page_index) OVER (PARTITION BY p.FILE_ID ORDER BY p.page_index) as prev_page
    from pages_with_packets p
),

-- Step 10: Group consecutive pages with the same packet number
packet_groups as (
    select
        *,
        SUM(CASE
            WHEN PACKETNUMBER = prev_packet AND page_index = prev_page + 1
            THEN 0 ELSE 1
        END) OVER (PARTITION BY FILE_ID ORDER BY page_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grp
    from all_pages_ordered
),

-- Step 11: Collapse groups into one row per packet occurrence
grouped_packets as (
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
        MAX(page_index) as PAGE_END
    from packet_groups
    group by PACKETNUMBER, FILE_ID, grp
),

-- Step 12: Extend PAGE_END to include supplementary pages
with_supplementary as (
    select
        gp.*,
        LEAD(gp.PAGE_INDEX) OVER (PARTITION BY gp.FILE_ID ORDER BY gp.PAGE_INDEX) as next_packet_start
    from grouped_packets gp
),

supplementary_max as (
    select
        ws.FILE_ID,
        ws.PAGE_INDEX,
        MAX(sp.page_index) as max_supp_page
    from with_supplementary ws
    inner join supplementary_pages sp
        on sp.FILE_ID = ws.FILE_ID
        and sp.page_index > ws.PAGE_END
        and sp.page_index < COALESCE(ws.next_packet_start, 999999)
    group by ws.FILE_ID, ws.PAGE_INDEX
),

final_pages as (
    select
        ws.PACKETNUMBER,
        ws.ACCOUNTCODE,
        ws.RECEIVEDDATE,
        ws.FILE_ID,
        ws.CREATED_AT,
        ws.MODIFIED_AT,
        ws._FIVETRAN_FILE_PATH,
        ws._FIVETRAN_SYNCED,
        ws.PAGE_INDEX,
        COALESCE(sm.max_supp_page, ws.PAGE_END) as PAGE_END
    from with_supplementary ws
    left join supplementary_max sm
        on sm.FILE_ID = ws.FILE_ID
        and sm.PAGE_INDEX = ws.PAGE_INDEX
),

-- Step 13: Enrich with Forge account code (live PACKET table only)
enriched as (
    select
        f.PACKETNUMBER,
        COALESCE(pk.ACCOUNTCODE, f.ACCOUNTCODE) as ACCOUNTCODE,
        COALESCE(f.RECEIVEDDATE, pk.COUNTERDATE) as RECEIVEDDATE,
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
)

-- Final output: exclude packets already in the legacy table, enforce one row per page
select PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, PAGE_INDEX, PAGE_END
from (
    select e.*,
        ROW_NUMBER() OVER (PARTITION BY e.FILE_ID, e.PAGE_INDEX ORDER BY e.PACKETNUMBER) as dedup_rn
    from enriched e
    where not exists (
        select 1
        from {{ source('sharepoint', 'HALLNOTES_PACKETNUMBER') }} hp
        where hp.PACKETNUMBER = e.PACKETNUMBER
          and hp.RECEIVEDDATE = e.RECEIVEDDATE
    )
)
where dedup_rn = 1