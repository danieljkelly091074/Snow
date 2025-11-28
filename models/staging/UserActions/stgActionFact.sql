{{ config(materilized='table')}}
WITH ActionFact As (

SELECT DISTINCT SiteKey, PACKETID AS PacketKey, DateKey, UserKey, ActionKey, TotalItems, TotalSampled
FROM {{ ref('stgUnionActions') }}  INNER JOIN {{ source('DBT_SNOWFLAKE', 'SITEDIM') }} ON dbo.SITEDIM.SiteDescription = stgUnionActions.SITEDESC 
INNER JOIN {{ source('DBT_SNOWFLAKE', 'USER_DIM') }}  ON User_Dim.UserName = stgUnionActions.UserName
INNER JOIN {{ source('DBT_SNOWFLAKE', 'ACTIONDIM') }} ON ACTIONDIM.ActionDescription = stgUnionActions.ActionDesc
INNER JOIN {{ ref('stgDateDim') }} ON stgDateDim.ActionDate = stgUnionActions.ActionDate 

)
SELECT * FROM ActionFact