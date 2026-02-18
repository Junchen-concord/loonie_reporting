USE LMSMaster

DROP TABLE IF EXISTS #LastActionNoAction

SELECT UserName, MAX(LogDate) AS LastLogDate INTO #LastActionNoAction FROM LoginAction
WHERE LogDate > '2025-07-08'
GROUP BY UserName

SELECT A.UserName, A.LastLogDate, L.[Action] FROM #LastActionNoAction A
LEFT JOIN LoginAction L ON A.UserName = L.UserName AND A.LastLogDate = L.LogDate
ORDER BY LastLogDate DESC


SELECT * FROM LoginAction
WHERE UserName = 'IBALAWR02'
AND LogDate > '2025-06-01'
ORDER BY LogDate DESC

