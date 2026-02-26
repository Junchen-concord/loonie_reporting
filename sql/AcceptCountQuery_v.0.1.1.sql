USE [LMSMaster];
GO

--Accept Count Alert (most updated stored procedure)
-- =============================================
-- Author:        Ryan Finazzo
-- Modified by: Junchen Xiong
-- =============================================
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

ALTER PROCEDURE [dbo].[USP_SystemAlert_AcceptCountProcedure]
    @dateRange INT,
    @timeRange INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Allow dirty reads for this reporting proc
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    --Active Lead Providers (last 3 days, based on ApplicationDate)
    SELECT DISTINCT
        LP.Provider_name
    FROM dbo.Application  AS A
    INNER JOIN dbo.LeadProvider AS LP
        ON A.LeadProviderID = LP.LeadProviderID
    WHERE A.ApplicationDate > DATEADD(DAY, -@dateRange, CAST(GETDATE() AS date));

    --Accepted Application Count per Provider
    SELECT
        COUNT(DISTINCT A.APPGUID) AS [ApplicationCount],
        LP.Provider_name
    FROM dbo.Application  AS A
    INNER JOIN dbo.LeadProvider AS LP
        ON A.LeadProviderID = LP.LeadProviderID
    WHERE A.DenialCode=0
        AND A.ApplicationDate BETWEEN
            DATEADD(HOUR, -@timeRange, DATEADD(DAY, -1, CAST(GETDATE() AS datetime)))
            AND DATEADD(DAY, -1, CAST(GETDATE() AS datetime))
    GROUP BY
        LP.Provider_name;
END;
GO
