-- Vision-based hallnotes packet extraction using claude-sonnet-4-6 on PDF directly
-- Vision-based hallnotes packet extraction using claude-sonnet-4-6 on PDF directly
-- Co-authored with CoCo
-- One API call per document. Files over 10MB are skipped (rescan in smaller batches).

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
        h._FIVETRAN_SYNCED,
        d.SIZE as file_size
    from {{ source('sharepoint', 'HALLNOTES') }} h
    inner join directory('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES') d
        on d.RELATIVE_PATH = h._FIVETRAN_FILE_PATH
    where h._FIVETRAN_FILE_PATH like 'root/%'
      and d.SIZE < 10000000  -- Skip files over 10MB (max ~50 pages at 200 DPI grayscale)
    {% if is_incremental() %}
      and not exists (
          select 1
          from {{ this }} p
          where p.FILE_ID = h.FILE_ID
      )
    {% endif %}
),

-- Step 1: Send entire PDF to claude-sonnet-4-6 for extraction
extracted as (
    select
        sf.*,
        AI_COMPLETE(
            'claude-sonnet-4-6',
            PROMPT('This is a scanned PDF of hallnotes from a jewellery assay office. For each page, extract:
- page_number (0-indexed)
- packet_number: the main barcode/packet number (5-10 chars, patterns: Letter+digits+suffix like S16582A, Q5342XB, N20528; Digits+suffix like 416467, 412931C; or pure digits like 412905). Read the barcode carefully and preserve ALL characters exactly.
- account_code: printed number after "Account No." or "Acc No:" or "Acc No." or "Account:" (4-6 digits only). Ignore handwritten codes. Ignore "Your Ref:" values.
- received_date: date at the VERY TOP of the page before the barcode, often prefixed with day of week (e.g. "Thu 30-Apr-2026"). Do NOT use "Est Comp" dates. Return in DD-Mon-YYYY format.
- is_supplementary: true if the page is a continuation/supplementary page belonging to the PREVIOUS packet. Supplementary pages include: Article Discrepancy Notes, Laser Engraving forms, Secondhand Check Sheets, invoice/delivery forms that reference the same packet as the preceding page. These do NOT start a new packet.

Return as a JSON array of objects. {0}',
                TO_FILE('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES', sf._FIVETRAN_FILE_PATH)
            ),
            response_format => {'type': 'json', 'schema': {
                'type': 'object',
                'properties': {
                    'pages': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                'page_number': {'type': 'integer'},
                                'packet_number': {'type': 'string'},
                                'account_code': {'type': 'string'},
                                'received_date': {'type': 'string'},
                                'is_supplementary': {'type': 'boolean'}
                            }
                        }
                    }
                }
            }}
        ) as llm_response
    from source_file sf
),

-- Step 2: Parse response and flatten to one row per page
parsed as (
    select
        e.FILE_ID,
        e.CREATED_AT,
        e.MODIFIED_AT,
        e._FIVETRAN_FILE_PATH,
        e._FIVETRAN_SYNCED,
        TRY_PARSE_JSON(e.llm_response) as response
    from extracted e
    where e.llm_response is not null
      and TRY_PARSE_JSON(e.llm_response) is not null
),

pages as (
    select
        p.FILE_ID,
        p.CREATED_AT,
        p.MODIFIED_AT,
        p._FIVETRAN_FILE_PATH,
        p._FIVETRAN_SYNCED,
        page.value:page_number::INT as page_index,
        UPPER(TRIM(page.value:packet_number::VARCHAR)) as raw_packetnumber,
        NULLIF(TRIM(page.value:account_code::VARCHAR), 'null') as raw_accountcode,
        page.value:received_date::VARCHAR as raw_date,
        COALESCE(page.value:is_supplementary::BOOLEAN, false) as is_supplementary
    from parsed p,
    lateral flatten(input => p.response:pages) page
),

-- Step 3: Clean and validate detections
detections as (
    select
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        page_index,
        -- Normalize packet number
        COALESCE(
            REGEXP_SUBSTR(raw_packetnumber, '^[A-Z]?[0-9]{4,}[A-Z]{0,2}$'),
            REGEXP_SUBSTR(raw_packetnumber, '[A-Z]?[0-9]{4,}[A-Z]{0,2}')
        ) as PACKETNUMBER,
        -- Validate account code (4-6 digits only)
        CASE
            WHEN REGEXP_LIKE(raw_accountcode, '^[0-9]{4,6}$') THEN raw_accountcode
            ELSE NULL
        END as ACCOUNTCODE,
        -- Parse received date
        COALESCE(
            TRY_TO_DATE(raw_date, 'DD-Mon-YYYY'),
            TRY_TO_DATE(raw_date, 'YYYY-MM-DD'),
            TRY_TO_DATE(raw_date, 'DD/MM/YYYY')
        ) as RECEIVEDDATE,
        is_supplementary
    from pages
    where raw_packetnumber is not null
      and raw_packetnumber != 'null'
      and LENGTH(raw_packetnumber) between 5 and 10
),

-- Step 4: Keep only non-supplementary pages with valid packets
valid_packets as (
    select *
    from detections
    where NOT is_supplementary
      and PACKETNUMBER is not null
      and REGEXP_LIKE(PACKETNUMBER, '^[A-Z]?[0-9]+[A-Z]{0,2}$')
      and LENGTH(PACKETNUMBER) between 5 and 10
    qualify ROW_NUMBER() OVER (PARTITION BY FILE_ID, page_index ORDER BY PACKETNUMBER) = 1
),

-- Step 5: Determine page ranges using supplementary page assignments
supplementary as (
    select
        FILE_ID,
        page_index,
        PACKETNUMBER
    from detections
    where is_supplementary
      and PACKETNUMBER is not null
),

packet_page_ranges as (
    select
        vp.FILE_ID,
        vp.PACKETNUMBER,
        vp.ACCOUNTCODE,
        vp.RECEIVEDDATE,
        vp.CREATED_AT,
        vp.MODIFIED_AT,
        vp._FIVETRAN_FILE_PATH,
        vp._FIVETRAN_SYNCED,
        vp.page_index as PAGE_INDEX,
        LEAD(vp.page_index) OVER (PARTITION BY vp.FILE_ID ORDER BY vp.page_index) as next_packet_start
    from valid_packets vp
),

-- Step 6: Calculate PAGE_END including supplementary pages
supplementary_max as (
    select
        ppr.FILE_ID,
        ppr.PAGE_INDEX,
        MAX(s.page_index) as max_supp_page
    from packet_page_ranges ppr
    inner join supplementary s
        on s.FILE_ID = ppr.FILE_ID
        and s.PACKETNUMBER = ppr.PACKETNUMBER
        and s.page_index > ppr.PAGE_INDEX
        and s.page_index < COALESCE(ppr.next_packet_start, 999999)
    group by ppr.FILE_ID, ppr.PAGE_INDEX
),

final_pages as (
    select
        ppr.PACKETNUMBER,
        ppr.ACCOUNTCODE,
        ppr.RECEIVEDDATE,
        ppr.FILE_ID,
        ppr.CREATED_AT,
        ppr.MODIFIED_AT,
        ppr._FIVETRAN_FILE_PATH,
        ppr._FIVETRAN_SYNCED,
        ppr.PAGE_INDEX,
        COALESCE(
            sm.max_supp_page,
            ppr.next_packet_start - 1,
            ppr.PAGE_INDEX
        ) as PAGE_END
    from packet_page_ranges ppr
    left join supplementary_max sm
        on sm.FILE_ID = ppr.FILE_ID
        and sm.PAGE_INDEX = ppr.PAGE_INDEX
),

-- Step 7: Enrich with Forge account code (live PACKET table only)
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

-- Final output: dedup and exclude legacy packets
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
