{{ config(materilized='table')}}

WITH ActionDate AS (
SELECT DISTINCT TO_CHAR(DATE, 'YYYYMMDD')::INT AS DATEKEY, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, DATE_PART(Day, Date) AS Day, DATE_PART(Month, Date) AS Month, DATE_PART(Year, Date) AS Year, DATE_PART(Quarter, Date) AS Quarter, DATE_PART(month, Date) AS MonthName, DATE_PART(weekday, 
                         Date) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }}
WHERE        (DATEDIFF(day, dbo.SupervisorActions.Date, GETDATE()) <= 1095)
UNION 
SELECT DISTINCT TO_CHAR(DATE, 'YYYYMMDD')::INT AS DATEKEY, TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, DATE_PART(Day, Date) AS Day, DATE_PART(Month, Date) AS Month, DATE_PART(Year, Date) AS Year, DATE_PART(Quarter, Date) AS Quarter, DATE_PART(month, Date) AS MonthName, DATE_PART(weekday, 
                         Date) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }}
WHERE        (DATEDIFF(day, dbo.ArchiveSupervisorActions.Date, GETDATE()) <= 1095)
)


select 
*
from ActionDate

