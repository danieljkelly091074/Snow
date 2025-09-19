with 

source as (

    select * from {{ source('UserActions', 'SUPERVISORACTIONS') }}

),

renamed as (

    select
        id,
        packetnumber,
        date,
        username,
        action,
        ip,
        hostname,
        aopcver

    from source

)

select * from renamed
