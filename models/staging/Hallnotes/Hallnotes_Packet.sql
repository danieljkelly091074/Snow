-- dbt incremental model: hallnotes_packetnumber.sql

{{
    config(
        materialized='incremental',
        unique_key='FILE_ID',
        schema='SHAREPOINT'
    )
}}

WITH source_files AS (
    SELECT 
        h.FILE_ID,
        h.CREATED_AT,
        h.MODIFIED_AT,
        h._FIVETRAN_FILE_PATH,
        h._FIVETRAN_SYNCED
    FROM {{ source('sharepoint', 'hallnotes') }} h
    {% if is_incremental() %}
    WHERE h.FILE_ID NOT IN (SELECT FILE_ID FROM {{ this }})
    {% endif %}
),

extracted AS (
    SELECT 
        FILE_ID,
        CREATED_AT,
        MODIFIED_AT,
        _FIVETRAN_FILE_PATH,
        _FIVETRAN_SYNCED,
        AI_EXTRACT(
            TO_FILE('@RAW__SHAREPOINT.SHAREPOINT.HALLNOTES', _FIVETRAN_FILE_PATH),
            PARSE_JSON('{"barcode_value": "What is the code shown as a barcode or find an alphanumeric code typically next to the barcode near a date at the top and does not have a letter except at the start or end of the text and is six to seven characters long?"}')
        ) AS result
    FROM source_files
)

SELECT 
    GET(GET(result, 'response'), 'barcode_value')::VARCHAR AS PACKETNUMBER,
    FILE_ID,
    CREATED_AT,
    MODIFIED_AT,
    _FIVETRAN_FILE_PATH,
    _FIVETRAN_SYNCED
FROM extracted
WHERE GET(GET(result, 'response'), 'barcode_value')::VARCHAR != 'None'
{% if is_incremental() %}
  AND GET(GET(result, 'response'), 'barcode_value')::VARCHAR NOT IN (
      SELECT PACKETNUMBER FROM {{ this }}
  )
{% endif %}