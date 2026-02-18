/*
Pattern based on `jcx_lending_guide/sql/get_data.sql`:
- Keep SQL in `sql/`
- Refresh script runs this query and writes outputs to `data/refresh/`

Expected output schema (single result set):
  GroupName   : e.g. 'Sales' | 'Performance'
  Metric      : KPI label
  Value       : KPI display value (string or numeric)
  Alert       : 'Green' | 'Yellow' | 'Red'
  Link        : URL (nullable)
  UpdatedAt   : timestamp for freshness
*/

USE LMSMaster
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
-- 3M applications during past 2 months
drop table if EXISTS #t
select A.Application_ID, A.PortFolioID, L.LoanID, case when A.LeadProviderID = 81 then SUBSTRING(
        A.PDLOANRCVDFROM, 
        CHARINDEX('_', A.PDLOANRCVDFROM, CHARINDEX('_', A.PDLOANRCVDFROM) + 1) + 1, 
        LEN(A.PDLOANRCVDFROM) - CHARINDEX('_', A.PDLOANRCVDFROM, CHARINDEX('_', A.PDLOANRCVDFROM) + 1)
    ) else A.PDLoanRcvdFrom end as PDLoanRcvdFrom,
CASE WHEN VW.Frequency in ('B','S') then 'B' else VW.Frequency end as Frequency, 
SA.DM_Band_Name, SA.CM_Band_Name, LP.Provider_name, 
-- CASE WHEN JA.UnderwritingStatus = 'Entered Scoring' then 1 else 0 end as UnderwritingstatusScored,
CASE WHEN A.ApplicationSteps like '%S%' then 1 else 0 end as UnderwritingstatusScored,
CASE WHEN A.DenialCode=0 then 1 else 0 end as Accepted,
CASE WHEN A.ESigstatus in ('V', 'S') THEN 1 ELSE 0 END AS Viewed,
CASE WHEN A.DenialCode=0 AND NOT A.LPCampaign='RETURN' then A.OfferPrice else 0 end as BidCost,
CASE WHEN ((L.LoanStatus NOT IN ('V', 'W', 'G', 'K')) and (L.LoanStatus is not null) and (A.DenialCode=0)) THEN 1 ELSE 0 END AS Originated,
CASE WHEN ((L.LoanStatus NOT IN ('V', 'W', 'G', 'K')) and (L.LoanStatus is not null) and (A.DenialCode=0)) THEN L.OriginatedAmount ELSE 0 END AS LoansFunded, 
L.OriginationDate, A.ApplicationDate, A.CustomerSSN, A.DenialCode, PF.FPD
into #t 
from application A
-- left join QlikDB..JeffApplication JA on A.Application_ID = JA.Application_ID and A.PortFolioID = JA.PortfolioID
left join LMS_Logs..VW_ApplicationDump VW on A.APPGUID = VW.APPGUID 
left join [QlikDB].[dbo].[ScoredApplications] SA on SA.PortFolioID = A.PortFolioID and A.Application_ID=SA.Application_ID
left join Loans L on A.PortfolioID=L.PortfolioID and A.Application_ID=L.ApplicationID
left join LeadProvider LP on A.LeadProviderID= LP.LeadProviderID
left JOIN DataFiles_US_Underwriting..US_Complete_Perf PF ON A.Application_ID=PF.Application_ID and A.PortFolioID=PF.PortfolioID 
-- Accept Rate: over last 30 days; Conversion Rate over last 60 days
where datediff(day,A.ApplicationDate, GETDATE()) between 0 and 7
and A.ApplicationSteps not like '%R%' and A.ApplicationSteps not like '%O%'


drop table if EXISTS #t1
select *,
CASE WHEN Provider_name='Zero Parallel' then PDLOANRCVDFROM 
     WHEN provider_name='Leads Market' then PDLOANRCVDFROM 
     when provider_name='IT Media' and ( PDLOANRCVDFROM like 'Xpart%' OR PDLOANRCVDFROM LIKE 'Xinter%') AND  (substring(PDLOANRCVDFROM ,len(PDLOANRCVDFROM)-1, 1)='X') THEN  (substring(PDLOANRCVDFROM , 1,len(PDLOANRCVDFROM) -1))   
     when provider_name='IT Media' and ( PDLOANRCVDFROM like 'Xpart%' OR PDLOANRCVDFROM LIKE 'Xinter%') AND  (substring(PDLOANRCVDFROM ,len(PDLOANRCVDFROM)-2, 1)='X') THEN  (substring(PDLOANRCVDFROM , 1,len(PDLOANRCVDFROM) -2))  
     when provider_name='IT Media' and ( PDLOANRCVDFROM like 'Xpart%' OR PDLOANRCVDFROM LIKE 'Xinter%') AND  (substring(PDLOANRCVDFROM ,len(PDLOANRCVDFROM)-3, 1)='X') THEN  (substring(PDLOANRCVDFROM , 1,len(PDLOANRCVDFROM) -3))
     WHEN provider_name IN ('Discover Nimbus','Leap Theory', 'Partner Weekly', 'Opaque', 'Ping Logix', 'IT Media', 'EPCVIP', 'FortuneX') then   (CASE WHEN PDLOANRCVDFROM LIKE '[-]%[-]%' THEN substring(PDLOANRCVDFROM, 2, Charindex('-', substring(PDLOANRCVDFROM,2,len(PDLOANRCVDFROM)-1))-1)
                 when (PDLOANRCVDFROM LIKE '[_]%[-]%') THEN substring(PDLOANRCVDFROM, 2, Charindex('-', PDLOANRCVDFROM) - 2)
                 when (PDLOANRCVDFROM LIKE '[-]%[_]%') THEN substring(PDLOANRCVDFROM, 2, Charindex('_', PDLOANRCVDFROM) - 2)
                 when (PDLOANRCVDFROM LIKE '[-]%[-]%') then substring(PDLOANRCVDFROM, 2, Charindex('-', substring(PDLOANRCVDFROM, 2, len(PDLOANRCVDFROM)-1))-1) 
                 when (PDLOANRCVDFROM LIKE '[_]%[_]%') then substring(PDLOANRCVDFROM, 2, Charindex('_', substring(PDLOANRCVDFROM, 2, len(PDLOANRCVDFROM)-1))-1) 
                 when (PDLOANRCVDFROM LIKE '%[-]%') and (substring(PDLOANRCVDFROM, 1, Charindex('-', PDLOANRCVDFROM)-1) = '') then PDLOANRCVDFROM
                 when (PDLOANRCVDFROM LIKE '%[-]%') and (substring(PDLOANRCVDFROM, 1, Charindex('-', PDLOANRCVDFROM)-1) <> '') then substring(PDLOANRCVDFROM, 1, Charindex('-', PDLOANRCVDFROM)-1)  
                 when (PDLOANRCVDFROM LIKE '%[_]%') and (substring(PDLOANRCVDFROM, 1, Charindex('_', PDLOANRCVDFROM)-1) = '') then PDLOANRCVDFROM
                 when (PDLOANRCVDFROM LIKE '%[_]%') and (substring(PDLOANRCVDFROM, 1, Charindex('_', PDLOANRCVDFROM)-1) <> '') then substring(PDLOANRCVDFROM, 1, Charindex('_', PDLOANRCVDFROM)-1)
                 else PDLOANRCVDFROM end)
      else PDLOANRCVDFROM  END as leadprovidersource
into #t1
from #t


SELECT TOP 10 *
FROM #t1

---------------------- Summarization -----------------------------------
SELECT 
CAST(ApplicationDate AS date) AS ActivityDate,
count(*) as Seen,
sum(UnderwritingstatusScored) as Scored,
sum(Accepted) as Accepted,
sum(Originated) as Originated,
SUM(CASE WHEN DenialCode IN (0, 115, 127) THEN 1 ELSE 0 END) AS Bids,
ROUND(CAST(SUM(CASE WHEN DenialCode IN (0, 115, 127) THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(SUM(UnderwritingstatusScored), 0), 4) AS BidRate,
ROUND(CAST(SUM(Accepted) AS FLOAT) / NULLIF(SUM(CASE WHEN DenialCode IN (0, 115, 127) THEN 1 ELSE 0 END), 0), 4) AS WinRate,
ROUND(CAST(SUM(UnderwritingstatusScored) AS FLOAT) / NULLIF(COUNT(*), 0), 4) AS ScoringRate,
ROUND(CAST(SUM(Accepted) AS FLOAT) / NULLIF(COUNT(*), 0), 4) as AcceptRate,
ROUND(CAST(SUM(Originated) AS FLOAT) / NULLIF(SUM(Accepted), 0), 4) as ConvRate,
sum(UnderwritingstatusScored) * 2.2 as ScoringCost,
sum(BidCost) as BidCost,
sum(LoansFunded) as LoansFunded,
ROUND(ISNULL(CAST(SUM(UnderwritingstatusScored)*2.2 AS FLOAT) / NULLIF(SUM(Accepted), 0), 999), 4) as ScoringCostPerAccepted,
ROUND(CAST(SUM(BidCost) AS Float)/NULLIF(SUM(Originated), 0), 4) AS BidCostPerOrig,
ROUND(CAST(SUM(BidCost) AS Float)/NULLIF(SUM(LoansFunded), 0), 4) AS BidCostPerFunded
FROM #t1
GROUP BY  CAST(ApplicationDate AS date)
ORDER BY CAST(ApplicationDate AS date) ASC;
