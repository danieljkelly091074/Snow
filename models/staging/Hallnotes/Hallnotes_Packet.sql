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
                'Extract the following fields from this document text. Return ONLY a valid JSON object with these exact keys: barcode_value, account_code, received_date. If a field is not found, use null.\n\n',
                'Rules:\n',
                '- barcode_value: Look for the MAIN packet/barcode number printed prominently, usually near the top of the page. It matches these patterns: (1) Letter(s) + digits + optional letter suffix like S16582A, S2271XB, N20528, Q53047A (2) Digits + letter suffix like 405599B, 412931C, 412834C (3) Pure 5-7 digit numbers like 412905. The code is 5-10 characters total. IMPORTANT: Preserve the FULL value exactly as printed including ALL leading letters. Do NOT drop or truncate any characters. Do NOT use the "Reg No" value. Do NOT include hallmark quality marks like "A+B", "A+C", "B+", etc. - if you see "A+B 412834C", the packet number is just "412834C". The text may have OCR noise - look carefully even if text is garbled. IMPORTANT: If the page contains "Article Discrepancy Note" text, it is a supplementary page belonging to the PREVIOUS packet - it does NOT start a new packet. The barcode on such pages is the SAME as the preceding packet.\n',
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
        CASE WHEN CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE') THEN true ELSE false END as is_supplementary
    from cleaned
    where result:barcode_value::VARCHAR is not null
      and result:barcode_value::VARCHAR != 'null'
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
        end as is_valid
    from all_detections_raw
    where PACKETNUMBER is not null
      and LENGTH(PACKETNUMBER) between 5 and 10
      and is_supplementary = false
),

valid_detections_raw as (
    select * from all_detections where is_valid
),

valid_detections as (
    select
        COALESCE(
            CASE
                WHEN REGEXP_LIKE(v.PACKETNUMBER, '^[A-Za-z][0-9]')
                     AND NOT EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
                     AND NOT EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER)
                     AND (EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
                          OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2)))
                THEN SUBSTR(v.PACKETNUMBER, 2)
                ELSE NULL
            END,
            v.PACKETNUMBER
        ) as PACKETNUMBER,
        v.ACCOUNTCODE, v.RECEIVEDDATE, v.FILE_ID, v.CREATED_AT, v.MODIFIED_AT, v._FIVETRAN_FILE_PATH, v._FIVETRAN_SYNCED, v.page_index, v.max_page_index, v.is_valid
    from valid_detections_raw v
    where EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER)
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
),

invalid_detections as (
    select * from all_detections where not is_valid
),

valid_deduplicated as (
    select
        PACKETNUMBER,
        ACCOUNTCODE,
        RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        MIN(page_index) as PAGE_INDEX,
        max_page_index,
        ROW_NUMBER() over (
            partition by PACKETNUMBER, FILE_ID
            order by
                case when RECEIVEDDATE is not null and ACCOUNTCODE is not null then 0
                     when RECEIVEDDATE is not null then 1
                     when ACCOUNTCODE is not null then 2
                     else 3
                end,
                page_index
        ) as rn
    from valid_detections
    group by PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, page_index, max_page_index
),

best_valid as (
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
        max_page_index,
        COALESCE(
            LEAD(PAGE_INDEX) OVER (PARTITION BY FILE_ID ORDER BY PAGE_INDEX) - 1,
            max_page_index
        ) as PAGE_END_RAW,
        LAG(PAGE_INDEX) OVER (PARTITION BY FILE_ID ORDER BY PAGE_INDEX) as prev_valid_start
    from valid_deduplicated
    where rn = 1
),

page_adjusted as (
    select
        bv.PACKETNUMBER,
        bv.ACCOUNTCODE,
        bv.RECEIVEDDATE,
        bv.FILE_ID,
        bv.CREATED_AT,
        bv.MODIFIED_AT,
        bv._FIVETRAN_FILE_PATH,
        bv._FIVETRAN_SYNCED,
        COALESCE(
            (select MIN(inv.page_index)
             from invalid_detections inv
             where inv.FILE_ID = bv.FILE_ID
               and inv.page_index < bv.PAGE_INDEX
               and inv.page_index > COALESCE(bv.prev_valid_start, -1)
               and (inv.ACCOUNTCODE = bv.ACCOUNTCODE or inv.PACKETNUMBER = bv.ACCOUNTCODE)
            ),
            bv.PAGE_INDEX
        ) as PAGE_INDEX,
        bv.max_page_index
    from best_valid bv
),

final_pages as (
    select
        pa.*,
        case
            when LEAD(pa.PAGE_INDEX) OVER (PARTITION BY pa.FILE_ID ORDER BY pa.PAGE_INDEX) is null
            then pa.max_page_index
            else LEAST(
                LEAD(pa.PAGE_INDEX) OVER (PARTITION BY pa.FILE_ID ORDER BY pa.PAGE_INDEX) - 1,
                pa.PAGE_INDEX + 5
            )
        end as PAGE_END
    from page_adjusted pa
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
