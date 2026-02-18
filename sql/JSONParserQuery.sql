USE SystemAlerts

DROP TABLE IF EXISTS #ParsedConvRate

-- Insert example alert data
INSERT INTO AlertJSONs(AlertType, AlertJSON)
VALUES ('ConvRate', '[{"OriginatedCountNEW": 547, "AcceptedCountNEW": 5231, "OriginatedCountRTG": 602, "AcceptedCountRTG": 835, "OriginatedCountRTO": 24, "AcceptedCountRTO": 146, "NEWConversionRate": 10.456891607723188, "RTGConversionRate": 72.09580838323353, "RTOConversionRate": 16.43835616438356, "NEWStatus": 0, "RTGStatus": 0, "RTOStatus": 1, "TimeOfAlert": "16:03:55 (02/07/25)"}]')

-- Show all alert data
SELECT * FROM AlertJSONs

DELETE FROM AlertJSONs

CREATE TABLE #ParsedConvRate (
    SourceID INT,
    EventName NVARCHAR(50),
    RecordTimestamp DATETIME,
    OriginatedCountNEW INT,
    AcceptedCountNEW INT,
    OriginatedCountRTG INT,
    AcceptedCountRTG INT,
    OriginatedCountRTO INT,
    AcceptedCountRTO INT,
    NEWConversionRate FLOAT,
    RTGConversionRate FLOAT,
    RTOConversionRate FLOAT,
    NEWStatus INT,
    RTGStatus INT,
    RTOStatus INT,
    TimeOfAlert NVARCHAR(50)
);

INSERT INTO #ParsedConvRate (
    SourceID, EventName, RecordTimestamp,
    OriginatedCountNEW, AcceptedCountNEW,
    OriginatedCountRTG, AcceptedCountRTG,
    OriginatedCountRTO, AcceptedCountRTO,
    NEWConversionRate, RTGConversionRate, RTOConversionRate,
    NEWStatus, RTGStatus, RTOStatus, TimeOfAlert
)
SELECT
    s.AlertID,
    s.AlertType,
    s.AlertDateCreated,
    j.[OriginatedCountNEW],
    j.[AcceptedCountNEW],
    j.[OriginatedCountRTG],
    j.[AcceptedCountRTG],
    j.[OriginatedCountRTO],
    j.[AcceptedCountRTO],
    j.[NEWConversionRate],
    j.[RTGConversionRate],
    j.[RTOConversionRate],
    j.[NEWStatus],
    j.[RTGStatus],
    j.[RTOStatus],
    j.[TimeOfAlert]
FROM AlertJSONs s
CROSS APPLY OPENJSON(s.AlertJSON)
WITH (
    OriginatedCountNEW INT,
    AcceptedCountNEW INT,
    OriginatedCountRTG INT,
    AcceptedCountRTG INT,
    OriginatedCountRTO INT,
    AcceptedCountRTO INT,
    NEWConversionRate FLOAT,
    RTGConversionRate FLOAT,
    RTOConversionRate FLOAT,
    NEWStatus INT,
    RTGStatus INT,
    RTOStatus INT,
    TimeOfAlert NVARCHAR(50)
) AS j;


SELECT * FROM #ParsedConvRate