{{ config(materialized="table") }}

with
    LineDetailDim as (
        select itemcode as externalid, 'Item' as linetype, item as linedetail
        from {{ source("DBT_SNOWFLAKE", "ITEM") }}
        union all
        select
            invoicelinecode as externalid,
            'AddService' as linetype,
            invoicelinetitle as linedetail
        from {{ source("DBT_SNOWFLAKE", "INVOICELINETITLE") }}
    )

select
    {{ dbt_utils.generate_surrogate_key(["ExternalID", "LineType"]) }} as linedetailid,
    *
from LineDetailDim
