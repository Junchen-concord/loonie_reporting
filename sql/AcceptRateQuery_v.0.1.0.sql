USE [LMSMaster];
GO

--Accept Rate Alert (most updated stored procedure)
-- =============================================
-- Author:        Ryan Finazzo
-- =============================================
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [dbo].[USP_SystemAlert_AcceptRateProcedure]
    @range INT,  -- Day offset (e.g., 1 = yesterday)
    @days  INT   -- Lookback window in days for dynamic thresholds
AS
BEGIN
    SET NOCOUNT ON;

    -- Allow dirty reads for this reporting proc
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    --TotalNet for day @range ago (excludes DenialCode = 100)
    SELECT
        COUNT(DISTINCT A.Application_ID) AS [TotalNet]
    FROM dbo.Application AS A
    WHERE CAST(A.ApplicationDate AS date) = DATEADD(DAY, -@range, CAST(GETDATE() AS date))
        AND A.DenialCode != 100;

    --TotalAccept for day @range ago
    SELECT
        COUNT(DISTINCT A.APPGUID) AS [TotalAccept]
    FROM dbo.Application AS A
    WHERE A.ApplicationStatus IN ('A', 'P')
        AND CAST(A.ApplicationDate AS date) = DATEADD(DAY, -@range, CAST(GETDATE() AS date));

    --Dynamic Thresholds - TotalNet (daily series over @days)
    SELECT
        COUNT(DISTINCT A.Application_ID)         AS [TotalNet],
        CAST(A.ApplicationDate AS date)          AS [Date]
    FROM dbo.Application AS A
    WHERE CAST(A.ApplicationDate AS date)
        BETWEEN DATEADD(DAY, -@days, CAST(GETDATE() AS date)) AND GETDATE()
        AND A.DenialCode != 100
    GROUP BY CAST(A.ApplicationDate AS date)
    ORDER BY CAST(A.ApplicationDate AS date);

    --Dynamic Thresholds - TotalAccept (daily series over @days)
    SELECT
        COUNT(DISTINCT A.APPGUID) AS [TotalAccept],
        CAST(A.ApplicationDate AS date) AS [Date]
    FROM dbo.Application AS A
    WHERE A.ApplicationStatus IN ('A', 'P')
        AND CAST(A.ApplicationDate AS date)
        BETWEEN DATEADD(DAY, -@days, CAST(GETDATE() AS date)) AND GETDATE()
    GROUP BY CAST(A.ApplicationDate AS date)
    ORDER BY CAST(A.ApplicationDate AS date);
END;
GO
