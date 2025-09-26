WITH CTE AS (
SELECT TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, DATE_PART(Day, Date) AS Day, DATE_PART(Month, Date) AS Month, DATE_PART(Year, Date) AS Year, DATE_PART(Quarter, Date) AS Quarter, DATE_PART(month, Date) AS MonthName, DATE_PART(weekday, 
                         Date) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'SUPERVISORACTIONS') }}
UNION ALL
SELECT TO_DATE(TO_VARCHAR(DATE, 'DD/MM/YYYY'),'DD/MM/YYYY') As ActionDate, DATE_PART(Day, Date) AS Day, DATE_PART(Month, Date) AS Month, DATE_PART(Year, Date) AS Year, DATE_PART(Quarter, Date) AS Quarter, DATE_PART(month, Date) AS MonthName, DATE_PART(weekday, 
                         Date) AS DayName
FROM {{ source('DBT_SNOWFLAKE', 'ARCHIVESUPERVISORACTIONS') }}
)


select 
*
from CTE

