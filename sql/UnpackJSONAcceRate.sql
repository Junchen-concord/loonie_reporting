USE SystemAlerts

SELECT
  t.[AlertID],
  j.[key],
  j.[value]
FROM [SystemAlerts].[dbo].[AlertJSONs] t
CROSS APPLY OPENJSON(t.[AlertJSON]) AS j
WHERE ISJSON(j.[value]) = 0 and t.AlertType='AcceRate'

