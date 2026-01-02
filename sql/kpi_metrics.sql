/*
KPI Metrics Refresh Query (MVP template)

Pattern based on `jcx_lending_guide/sql/get_data.sql`:
- Keep SQL in `sql/`
- Refresh script runs this query and writes outputs to `data/refresh/`

Replace the placeholders with your real tables/logic.

Expected output schema (single result set):
  GroupName   : e.g. 'Sales' | 'Performance'
  Metric      : KPI label
  Value       : KPI display value (string or numeric)
  Alert       : 'Green' | 'Yellow' | 'Red'
  Link        : URL (nullable)
  UpdatedAt   : timestamp for freshness
*/

SELECT
  'Sales' AS GroupName,
  'Accept Count' AS Metric,
  CAST(NULL AS VARCHAR(64)) AS Value,
  'Green' AS Alert,
  CAST(NULL AS VARCHAR(2048)) AS Link,
  GETDATE() AS UpdatedAt
WHERE 1 = 0;


