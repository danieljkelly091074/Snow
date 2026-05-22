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

hallnote_boundaries as (
    select
        FILE_ID,
        page_index,
        LEAD(page_index) OVER (PARTITION BY FILE_ID ORDER BY page_index) as next_boundary
    from cleaned
    where CONTAINS(UPPER(page_content), 'HALLNOTE')
      and CONTAINS(UPPER(page_content), 'PACKET TYPE:')
      and NOT CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE')
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
        page_content,
        CASE WHEN CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY NOTE') THEN true ELSE false END as is_supplementary
    from cleaned
    where result:barcode_value::VARCHAR is not null
      and result:barcode_value::VARCHAR != 'null'
),

regex_fallback as (
    select
        UPPER(REGEXP_SUBSTR(page_content, '\\b([A-Za-z][0-9]{4,6}[A-Za-z]?)\\b', 1, 1, 'e')) as PACKETNUMBER,
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
        v.ACCOUNTCODE, v.RECEIVEDDATE, v.FILE_ID, v.CREATED_AT, v.MODIFIED_AT, v._FIVETRAN_FILE_PATH, v._FIVETRAN_SYNCED, v.page_index, v.max_page_index, v.is_valid, v.is_hallnote_header
    from valid_detections_raw v
    where EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = v.PACKETNUMBER)
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = v.PACKETNUMBER)
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'PACKET') }} p WHERE p.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
       OR EXISTS (SELECT 1 FROM {{ source('forge', 'ARCHIVEPACKET') }} ap WHERE ap.PACKETNUMBER = SUBSTR(v.PACKETNUMBER, 2))
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
    from valid_detections_corrected
    where is_corrected_header = false
    group by PACKETNUMBER, ACCOUNTCODE, RECEIVEDDATE, FILE_ID, CREATED_AT, MODIFIED_AT, _FIVETRAN_FILE_PATH, _FIVETRAN_SYNCED, page_index, max_page_index
),

contiguous_pages as (
    select
        vdc.PACKETNUMBER,
        vdc.FILE_ID,
        vdc.page_index,
        vdc.is_valid,
        COALESCE(
            SUM(CASE WHEN hb.page_index IS NOT NULL THEN 1 ELSE 0 END)
                OVER (PARTITION BY vdc.PACKETNUMBER, vdc.FILE_ID ORDER BY vdc.page_index
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            0
        ) as header_group
    from valid_detections_corrected vdc
    left join hallnote_boundaries hb
        on hb.FILE_ID = vdc.FILE_ID and hb.page_index = vdc.page_index
    where vdc.is_corrected_header = false
),

first_contiguous_group as (
    select
        PACKETNUMBER,
        FILE_ID,
        MIN(page_index) as first_page,
        MAX(page_index) as last_page_in_group
    from contiguous_pages
    where header_group = 1
    group by PACKETNUMBER, FILE_ID
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
        COALESCE(
            LEAD(vd.PAGE_INDEX) OVER (PARTITION BY vd.FILE_ID ORDER BY vd.PAGE_INDEX) - 1,
            vd.max_page_index
        ) as PAGE_END_RAW,
        LAG(vd.PAGE_INDEX) OVER (PARTITION BY vd.FILE_ID ORDER BY vd.PAGE_INDEX) as prev_valid_start,
        fcg.last_page_in_group
    from valid_deduplicated vd
    left join first_contiguous_group fcg
        on fcg.PACKETNUMBER = vd.PACKETNUMBER and fcg.FILE_ID = vd.FILE_ID
    where vd.rn = 1
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
            (select MAX(hb.page_index)
             from hallnote_boundaries hb
             where hb.FILE_ID = bv.FILE_ID
               and hb.page_index < bv.PAGE_INDEX
               and hb.page_index > COALESCE(bv.prev_valid_start, -1)
            ),
            (select MIN(inv.page_index)
             from invalid_detections inv
             where inv.FILE_ID = bv.FILE_ID
               and inv.page_index < bv.PAGE_INDEX
               and inv.page_index > COALESCE(bv.prev_valid_start, -1)
               and (inv.ACCOUNTCODE = bv.ACCOUNTCODE or inv.PACKETNUMBER = bv.ACCOUNTCODE)
            ),
            bv.PAGE_INDEX
        ) as PAGE_INDEX,
        bv.max_page_index,
        bv.last_page_in_group
    from best_valid bv
),

page_adjusted_with_end as (
    select
        pa.*,
        COALESCE(
            LEAD(pa.PAGE_INDEX) OVER (PARTITION BY pa.FILE_ID ORDER BY pa.PAGE_INDEX) - 1,
            pa.max_page_index
        ) as max_possible_end
    from page_adjusted pa
),

hallnote_end_boundary as (
    select
        pae.PACKETNUMBER,
        pae.FILE_ID,
        pae.PAGE_INDEX,
        pae.max_possible_end,
        MIN(CASE
            WHEN hb_pkt.boundary_packet != pae.PACKETNUMBER
                 AND hb_pkt.page_index > pae.PAGE_INDEX
            THEN hb_pkt.page_index - 1
            ELSE NULL
        END) as end_by_next_diff_hallnote,
        MIN(CASE
            WHEN hb_pkt.boundary_packet = pae.PACKETNUMBER
                 AND hb_pkt.page_index > pae.PAGE_INDEX
            THEN hb_pkt.page_index
            ELSE NULL
        END) as own_hallnote_after_start,
        MAX(CASE
            WHEN hb_pkt.boundary_packet = pae.PACKETNUMBER
                 AND hb_pkt.page_index = pae.PAGE_INDEX
            THEN true
            ELSE false
        END) as start_is_own_hallnote,
        MAX(CASE
            WHEN c_supp.page_index IS NOT NULL THEN true
            ELSE false
        END) as has_supplementary_content
    from page_adjusted_with_end pae
    left join (
        select
            c.FILE_ID,
            c.page_index,
            UPPER(COALESCE(
                REGEXP_SUBSTR(REPLACE(TRIM(c.result:barcode_value::VARCHAR), ' ', ''), '[0-9]{4,}[A-Za-z]{0,2}'),
                c.result:barcode_value::VARCHAR
            )) as boundary_packet
        from cleaned c
        where CONTAINS(UPPER(c.page_content), 'HALLNOTE')
          and CONTAINS(UPPER(c.page_content), 'PACKET TYPE:')
          and NOT CONTAINS(UPPER(c.page_content), 'ARTICLE DISCREPANCY NOTE')
    ) hb_pkt
        on hb_pkt.FILE_ID = pae.FILE_ID
    left join (
        select FILE_ID, page_index
        from cleaned
        where CONTAINS(UPPER(page_content), 'ARTICLE DISCREPANCY')
    ) c_supp
        on c_supp.FILE_ID = pae.FILE_ID
        and c_supp.page_index > pae.PAGE_INDEX
        and c_supp.page_index <= pae.max_possible_end
    group by pae.PACKETNUMBER, pae.FILE_ID, pae.PAGE_INDEX, pae.max_possible_end
),

final_pages as (
    select
        pae.PACKETNUMBER,
        pae.ACCOUNTCODE,
        pae.RECEIVEDDATE,
        pae.FILE_ID,
        pae.CREATED_AT,
        pae.MODIFIED_AT,
        pae._FIVETRAN_FILE_PATH,
        pae._FIVETRAN_SYNCED,
        pae.PAGE_INDEX,
        pae.max_page_index,
        pae.last_page_in_group,
        LEAST(
            COALESCE(
                CASE
                    WHEN heb.start_is_own_hallnote = false
                         AND heb.own_hallnote_after_start IS NOT NULL
                         AND heb.own_hallnote_after_start <= pae.max_possible_end
                         AND heb.has_supplementary_content = false
                    THEN heb.own_hallnote_after_start
                    WHEN heb.start_is_own_hallnote = true
                         AND heb.own_hallnote_after_start IS NOT NULL
                         AND heb.own_hallnote_after_start <= pae.max_possible_end
                    THEN heb.own_hallnote_after_start - 1
                    ELSE NULL
                END,
                heb.end_by_next_diff_hallnote,
                pae.max_possible_end,
                LEAST(pae.max_page_index, pae.PAGE_INDEX + 5)
            ),
            COALESCE(heb.end_by_next_diff_hallnote, pae.max_possible_end, LEAST(pae.max_page_index, pae.PAGE_INDEX + 5)),
            COALESCE(pae.max_possible_end, LEAST(pae.max_page_index, pae.PAGE_INDEX + 5))
        ) as PAGE_END
    from page_adjusted_with_end pae
    left join hallnote_end_boundary heb
        on heb.FILE_ID = pae.FILE_ID
        and heb.PACKETNUMBER = pae.PACKETNUMBER
        and heb.PAGE_INDEX = pae.PAGE_INDEX
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
