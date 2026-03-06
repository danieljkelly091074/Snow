{% snapshot orders_snapshot %}
{{ config(
    target_schema='snapshots',
    strategy='check',
    unique_key='CustomerID',
    check_cols=['AccountCode', 'CustomerName', 'Address1', 'Address2', 'Address3', 'Country', 'Postcode']
) }}
select * from {{ source('DBT_SNOWFLAKE', 'CUSTOMER') }}
{% endsnapshot %}
