-- dbt incremental model: hallnotes_packetnumber.sql

{{
    config(
        materialized='incremental',
        unique_key='FILE_ID',
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
            {'mode': 'OCR', 'page_filter': [{'start': 0, 'end': 1}]}
        ) as doc
    from source_file sf
),

extracted as (
    select
        p.*,
        SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-llama-3.3-70b',
            CONCAT(
                'Extract the following fields from this document text. Return ONLY a valid JSON object with these exact keys: barcode_value, account_code, received_date. If a field is not found, use null.\n\n',
                'Rules:\n',
                '- barcode_value: IMPORTANT - Look for a code matching these patterns: (1) Letter + 5 digits like N20528, X21295, A12345 (2) 6 digits + letter like 405599B, 123456A (3) Pure 6-7 digit numbers. Usually appears prominently near the top, often near a date or weight. The code is 5-10 characters. Look for the large number printed prominently at the top of the hallnote (this is the packet number, e.g. 407745). Do NOT use the "Reg No" value.\n',
                '- account_code: Look for a PRINTED/TYPED number specifically after "Account No." or "Acc No." or "Acc No:". IGNORE any handwritten account codes - only use machine-printed text. IGNORE any values after "Your Ref" or "Your Ref:" - these are NOT account codes. The account code should be a clean numeric string (e.g., 082536, 072889) with no decimal points, letters, or slashes. If the value contains letters or slashes (like EA01740/26) it is a "Your Ref" not an account code - return null. If it appears handwritten (e.g., decimal points like 282.536), return null.\n',
                '- received_date: Look for a date in DD-Mon-YYYY format (e.g., 04-Mar-2026, 05-Mar-2026) near the top of the document OR after "Received:". Return in DD-Mon-YYYY format.\n\n',
                'Document text:\n',
                doc:pages[0]:content::VARCHAR
            )
        ) as llm_response
    from parsed p
),

cleaned as (
    select
        e.*,
        TRY_PARSE_JSON(REGEXP_SUBSTR(llm_response, '\\{[^{}]*\\}')) as result
    from extracted e
),

filtered as (
    select
        result:barcode_value::VARCHAR as PACKETNUMBER,
        NULLIF(REPLACE(result:account_code::VARCHAR, ' ', ''), 'null') as ACCOUNTCODE,
        TRY_TO_DATE(result:received_date::VARCHAR, 'DD-Mon-YYYY') as RECEIVEDDATE,
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED
    from cleaned
    where result:barcode_value::VARCHAR is not null
      and result:barcode_value::VARCHAR != 'null'
      and LENGTH(result:barcode_value::VARCHAR) between 5 and 10
),

enriched as (
    select
        f.PACKETNUMBER,
        COALESCE(f.ACCOUNTCODE, pk.ACCOUNTCODE, apk.ACCOUNTCODE) as ACCOUNTCODE,
        f.RECEIVEDDATE,
        f.FILE_ID,
        f.CREATED_AT,
        f.MODIFIED_AT,
        f._FIVETRAN_FILE_PATH,
        f._FIVETRAN_SYNCED
    from filtered f
    left join (
        select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE
        from {{ source('forge', 'PACKET') }}
    ) pk
        on pk.PACKETNUMBER = f.PACKETNUMBER
        and pk.COUNTERDATE = f.RECEIVEDDATE
    left join (
        select PACKETNUMBER, TRADESMANACCOUNTCODE as ACCOUNTCODE, COUNTER::DATE as COUNTERDATE
        from {{ source('forge', 'ARCHIVEPACKET') }}
    ) apk
        on apk.PACKETNUMBER = f.PACKETNUMBER
        and apk.COUNTERDATE = f.RECEIVEDDATE
)

select * from enriched e
where not exists (
    select 1
    from {{ source('sharepoint','HALLNOTES_PACKETNUMBER') }} hp
    where hp.PACKETNUMBER = e.PACKETNUMBER
      and hp.RECEIVEDDATE = e.RECEIVEDDATE
)