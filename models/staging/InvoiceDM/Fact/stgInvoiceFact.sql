{{ config(
    materialized='incremental',
    unique_key=['LineDetailKey', 'DateKey', 'AccountKey', 'HeaderKey']
) }}

WITH InvoiceFact AS (
    SELECT
        ld.LineDetailID AS LineDetailKey,
        dd.DATEKEY AS DateKey,
        cd.AccountCode AS AccountKey,
        hd.HEADERID AS HeaderKey,
        pi.InvoiceQuantity AS Quantity,
        pi.ItemPricePerItem AS Price,
        p.VATRate AS VAT
    FROM {{ source('DBT_SNOWFLAKE', 'PACKET') }} p
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKETITEM') }} pi
        ON p.PacketNumber = pi.PacketNumber
    INNER JOIN stgInvoiceLineDetailDim ld
        ON pi.ItemCode = ld.ExternalID
    INNER JOIN stgInvoiceCustomerDim cd
        ON p.TradesmanAccountCode = cd.AccountCode
    INNER JOIN stgInvoiceHeaderDim hd
        ON p.PacketID = hd.HeaderID
    INNER JOIN stgInvoiceDateDim dd
        ON dd.Date = CAST(p.Packed AS DATE)
    WHERE (pi.InvoiceQuantity * pi.ItemPricePerItem <> 0) AND (ld.LineType = 'Item')

    UNION ALL

    SELECT
        ld.LineDetailID AS LineDetailKey,
        dd.DATEKEY AS DateKey,
        cd.AccountCode AS AccountKey,
        hd.HEADERID AS HeaderKey,
        api.InvoiceQuantity AS Quantity,
        api.ItemPricePerItem AS Price,
        ap.VATRate AS VAT
    FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ap
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKETITEM') }} api
        ON ap.PacketID = api.PacketID
    INNER JOIN stgInvoiceLineDetailDim ld
        ON api.ItemCode = ld.ExternalID
    INNER JOIN stgInvoiceCustomerDim cd
        ON ap.TradesmanAccountCode = cd.AccountCode
    INNER JOIN stgInvoiceHeaderDim hd
        ON ap.PacketID = hd.HeaderID
    INNER JOIN stgInvoiceDateDim dd
        ON dd.Date = CAST(ap.Packed AS DATE)
    WHERE (api.InvoiceQuantity * api.ItemPricePerItem <> 0) AND (ld.LineType = 'Item')

    UNION ALL

    SELECT
        ld.LineDetailID AS LineDetailKey,
        dd.DATEKEY AS DateKey,
        cd.AccountCode AS AccountKey,
        hd.HEADERID AS HeaderKey,
        pas.InvoiceQuantity AS Quantity,
        pas.ServicePricePerItem AS Price,
        p.VATRate AS VAT
    FROM {{ source('DBT_SNOWFLAKE', 'PACKETADDITIONALSERVICE') }} pas
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'PACKET') }} p
        ON pas.PacketNumber = p.PacketNumber
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'ADDITIONALSERVICE') }} asv
        ON pas.AdditionalServiceID = asv.AdditionalServiceID
    INNER JOIN stgInvoiceLineDetailDim ld
        ON asv.InvoiceTitleCode = ld.ExternalID
    INNER JOIN stgInvoiceCustomerDim cd
        ON p.TradesmanAccountCode = cd.AccountCode
    INNER JOIN stgInvoiceHeaderDim hd
        ON p.PacketID = hd.HeaderID
    INNER JOIN stgInvoiceDateDim dd
        ON dd.Date = CAST(p.Packed AS DATE)
    WHERE (ld.LineType = 'AddService')

    UNION ALL

    SELECT
        ld.LineDetailID AS LineDetailKey,
        dd.DATEKEY AS DateKey,
        cd.AccountCode AS AccountKey,
        hd.HEADERID AS HeaderKey,
        apas.InvoiceQuantity AS Quantity,
        apas.ServicePricePerItem AS Price,
        ap.VATRate AS VAT
    FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKETADDITIONALSERVICE') }} apas
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'ARCHIVEPACKET') }} ap
        ON apas.PacketID = ap.PacketID
    INNER JOIN {{ source('DBT_SNOWFLAKE', 'ADDITIONALSERVICE') }} asv
        ON apas.AdditionalServiceID = asv.AdditionalServiceID
    INNER JOIN stgInvoiceLineDetailDim ld
        ON asv.InvoiceTitleCode = ld.ExternalID
    INNER JOIN stgInvoiceCustomerDim cd
        ON ap.TradesmanAccountCode = cd.AccountCode
    INNER JOIN stgInvoiceHeaderDim hd
        ON ap.PacketID = hd.HeaderID
    INNER JOIN stgInvoiceDateDim dd
        ON dd.Date = CAST(ap.Packed AS DATE)
    WHERE (ld.LineType = 'AddService')
)

SELECT * FROM InvoiceFact

{% if is_incremental() %}
WHERE (LineDetailKey, DateKey, AccountKey, HeaderKey) NOT IN (
    SELECT LineDetailKey, DateKey, AccountKey, HeaderKey FROM {{ this }}
)
{% endif %}