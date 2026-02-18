USE LMSMaster

--Conversion Rate Alert (most updated stored procedure)
-- =============================================
-- Author:        Ryan Finazzo
-- =============================================
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [dbo].[USP_SystemAlert_ConversionRateProcedure]
    @startNum INT,
    @endNum   INT,
    @days     INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Allow dirty reads for this reporting proc
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    --Originated Alert Data
    SELECT
        SUM(CASE WHEN A.ApplicationSteps NOT LIKE '%R%'AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Originated Loans for NEW customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Originated Loans for RTG customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%O%' AND A.ApplicationSteps NOT LIKE '%R%' THEN 1 ELSE 0 END) AS [Originated Loans for RTO customers]
    FROM dbo.Loans AS L
    INNER JOIN dbo.Application AS A
        ON L.ApplicationID = A.Application_ID
        AND L.PortFolioID = A.PortFolioID
    WHERE L.LoanStatus NOT IN ('V', 'W', 'G', 'K')
        AND CONVERT(date, A.ApplicationDate)
            BETWEEN DATEADD(DAY, -@startNum, CAST(GETDATE() AS date))
                AND DATEADD(DAY, -@endNum, CAST(GETDATE() AS date));

    --Accepted Alert Data
   SELECT
        SUM(CASE WHEN A.ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Accepted NEW customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%R%' THEN 1 ELSE 0 END) AS [Accepted RTG customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%O%' AND A.ApplicationSteps NOT LIKE '%R%' THEN 1 ELSE 0 END) AS [Accepted RTO customers]
    FROM dbo.Application AS A
    WHERE A.ApplicationStatus IN ('A', 'P')
        AND CONVERT(date, A.ApplicationDate)
            BETWEEN DATEADD(DAY, -@startNum, CAST(GETDATE() AS date))
                AND DATEADD(DAY, -@endNum, CAST(GETDATE() AS date));

    --Data Search Start Day
    SELECT DATEADD(DAY, -@startNum, CAST(GETDATE() AS date));

    --Data Search End Day
    SELECT DATEADD(DAY, -@endNum, CAST(GETDATE() AS date));

    --Dynamic Thresholds - Originated
    SELECT
        SUM(CASE WHEN A.ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Originated Loans for NEW customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%R%'  AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Originated Loans for RTG customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%O%'  AND A.ApplicationSteps NOT LIKE '%R%' THEN 1 ELSE 0 END) AS [Originated Loans for RTO customers],
        CAST(A.ApplicationDate AS date) AS [ApplicationDate]
    FROM dbo.Loans AS L
    INNER JOIN dbo.Application AS A
        ON L.ApplicationID = A.Application_ID
        AND L.PortFolioID = A.PortFolioID
    WHERE L.LoanStatus NOT IN ('V', 'W', 'G', 'K')
        AND A.ApplicationDate BETWEEN DATEADD(DAY, -@days, CAST(GETDATE() AS date)) AND GETDATE()
    GROUP BY CAST(A.ApplicationDate AS date)
    ORDER BY CAST(A.ApplicationDate AS date);

    --Dynamic Thresholds - Accepted
    SELECT
        SUM(CASE WHEN A.ApplicationSteps NOT LIKE '%R%' AND A.ApplicationSteps NOT LIKE '%O%' THEN 1 ELSE 0 END) AS [Accepted NEW customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%R%' THEN 1 ELSE 0 END) AS [Accepted RTG customers],
        SUM(CASE WHEN A.ApplicationSteps LIKE '%O%' AND A.ApplicationSteps NOT LIKE '%R%' THEN 1 ELSE 0 END) AS [Accepted RTO customers],
        CAST(A.ApplicationDate AS date) AS [ApplicationDate]
    FROM dbo.Application AS A
    WHERE A.ApplicationStatus IN ('A', 'P')
        AND A.ApplicationDate BETWEEN DATEADD(DAY, -@days, CAST(GETDATE() AS date)) AND GETDATE()
    GROUP BY CAST(A.ApplicationDate AS date)
    ORDER BY CAST(A.ApplicationDate AS date);
END;
GO
