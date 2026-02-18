USE LMSMaster

-- Part 1; Gather data on transaction and payment level, append filtering conditions.
DROP TABLE IF EXISTS #LoanDefault
SELECT L.LoanID, L.ApplicationID AS Application_ID, A.ApplicationDate, A.ApplicationSteps, L.PortFolioID, L.LoanStatus,
P.InstallmentNumber, P.PaymentStatus, P.PaymentType, P.PaymentMode, P.AttemptNo, P.TransactionDate, P.PaymentID,
I.InstallmentID, I.iPaymentMode, I.DueDate, I.Status, -- used to exclude pendings (code 684)
(CASE WHEN I.Status=684 THEN 1 ELSE 0 END) AS Pending
INTO #LoanDefault
FROM LMSMaster..Loans L
LEFT JOIN LMSMaster..Payment P ON P.LoanID = L.LoanID
LEFT JOIN LMSMaster..Installments I ON I.InstallmentID = P.InstallmentID
LEFT JOIN LMSMaster..Application A ON A.PortfolioID=L.PortfolioID AND A.Application_ID = L.ApplicationID
WHERE A.ApplicationDate >= '2024-08-01' AND A.ApplicationDate < '2025-08-01' 
AND I.InstallmentNumber = 1

-- Part 2; Adding FPDFA Flag.
DROP TABLE IF EXISTS #LoanDefault_Flag
SELECT 
    L.*,
    -- FPDFA flag
    CASE 
        WHEN L.PaymentStatus = 'R'
             AND L.PaymentType IN ('I','S','A')
             AND L.PaymentMode IN ('A','B','D')
             AND L.DueDate <= CAST(GETDATE() AS date)
             AND NOT EXISTS (
                 SELECT 1
                 FROM #LoanDefault ld
                 WHERE ld.InstallmentID = L.InstallmentID
                   AND ld.PaymentStatus = 'D'
                   AND ld.PaymentType NOT IN ('3','~','Q')
                   AND ld.PaymentMode IN ('A','D','B')
                   AND CONVERT(date, ld.TransactionDate) = CONVERT(date, L.DueDate)
             )
        THEN 1 ELSE 0 END AS is_FPDFA,
        CASE 
        WHEN L.LoanStatus NOT IN ('V','W','G','K')
             AND NOT (
                 L.iPaymentMode = 144 
                 AND L.Pending = 1
                 AND L.DueDate >= CAST(GETDATE() AS date)
             )
        THEN 1 ELSE 0 
    END AS is_loan_first_install
INTO #LoanDefault_Flag
FROM #LoanDefault L;

-- Part 3; Deduplicate at loan level, use is_FPDFA Flag.  
DROP TABLE IF EXISTS #LoanDefault_Dedup;
WITH dedup AS (
    SELECT LoanID, Application_ID, ApplicationDate, ApplicationSteps, PortfolioID, LoanStatus,
           InstallmentNumber, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
           PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install,
           ROW_NUMBER() OVER (PARTITION BY Application_ID, PortfolioID ORDER BY is_FPDFA DESC) AS rn
    FROM #LoanDefault_Flag
)
SELECT LoanID, Application_ID, ApplicationDate, ApplicationSteps, PortfolioID, LoanStatus,
       InstallmentNumber, PaymentStatus, PaymentType, PaymentMode, AttemptNo, TransactionDate,
       PaymentID, InstallmentID, iPaymentMode, DueDate, Status, Pending, is_FPDFA, is_loan_first_install
INTO #LoanDefault_Dedup
FROM dedup
WHERE rn = 1;

-- Calculates Weekly FPDFA
SELECT 
    DATEPART(YEAR, ApplicationDate) AS AppYear,
    DATEPART(WEEK, ApplicationDate) AS AppWeek,

    COUNT(DISTINCT CASE WHEN is_FPDFA = 1 THEN LoanID END) AS FPDFA_count_all,
    COUNT(DISTINCT CASE WHEN is_loan_first_install = 1 THEN LoanID END) AS first_install_loan_count_all,

    COUNT(DISTINCT CASE 
        WHEN is_FPDFA = 1 
         AND ApplicationSteps NOT LIKE '%R%' 
         AND ApplicationSteps NOT LIKE '%O%' 
        THEN LoanID END
    ) AS FPDFA_count_new,

    COUNT(DISTINCT CASE 
        WHEN is_loan_first_install = 1 
         AND ApplicationSteps NOT LIKE '%R%' 
         AND ApplicationSteps NOT LIKE '%O%' 
        THEN LoanID END
    ) AS first_install_loan_count_new,

    COUNT(DISTINCT CASE 
        WHEN is_FPDFA = 1 
         AND (ApplicationSteps LIKE '%R%' OR ApplicationSteps LIKE '%O%') 
        THEN LoanID END
    ) AS FPDFA_count_return,

    COUNT(DISTINCT CASE 
        WHEN is_loan_first_install = 1 
         AND (ApplicationSteps LIKE '%R%' OR ApplicationSteps LIKE '%O%') 
        THEN LoanID END
    ) AS first_install_loan_count_return,

    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN is_FPDFA = 1 THEN LoanID END) AS FLOAT) 
        / NULLIF(COUNT(DISTINCT CASE WHEN is_loan_first_install = 1 THEN LoanID END), 0), 
    4) AS FPDFA_rate_all,

    ROUND(
        CAST(COUNT(DISTINCT CASE 
            WHEN is_FPDFA = 1 
             AND ApplicationSteps NOT LIKE '%R%' 
             AND ApplicationSteps NOT LIKE '%O%' 
            THEN LoanID END
        ) AS FLOAT) 
        / NULLIF(COUNT(DISTINCT CASE 
            WHEN is_loan_first_install = 1 
             AND ApplicationSteps NOT LIKE '%R%' 
             AND ApplicationSteps NOT LIKE '%O%' 
            THEN LoanID END
        ), 0),
    4) AS FPDFA_rate_new,

    ROUND(
        CAST(COUNT(DISTINCT CASE 
            WHEN is_FPDFA = 1 
             AND (ApplicationSteps LIKE '%R%' OR ApplicationSteps LIKE '%O%') 
            THEN LoanID END
        ) AS FLOAT) 
        / NULLIF(COUNT(DISTINCT CASE 
            WHEN is_loan_first_install = 1 
             AND (ApplicationSteps LIKE '%R%' OR ApplicationSteps LIKE '%O%') 
            THEN LoanID END
        ), 0),
    4) AS FPDFA_rate_return

FROM #LoanDefault_Flag
GROUP BY 
    DATEPART(YEAR, ApplicationDate),
    DATEPART(WEEK, ApplicationDate)
ORDER BY 
    AppYear,
    AppWeek;
