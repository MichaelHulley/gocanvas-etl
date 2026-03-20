-- =============================================
-- Daily Management Report Query (FIXED)
-- Uses temp table instead of CTE
-- =============================================
SET NOCOUNT ON;
--DECLARE @ReportDate date = CAST(GETDATE() AS date);
 DECLARE @ReportDate date = '2026-03-14';

-- Clean up if rerun
IF OBJECT_ID('tempdb..#calc') IS NOT NULL
    DROP TABLE #calc;

-- =============================================
-- BUILD DATASET
-- =============================================
SELECT
    submission_id,
    form_id,
    submission_number,
    submission_name,
    created_at_utc,
    intake_date,
    intake_time,
    load_number,
    grower_name,
    farm_name,
    weighbridge_net_weight_kg,
    rotten_damaged_weight_kg,
    rotten_damaged_pct,
    good_quality_brix,
    good_quality_ph,
    usd_total_value,
    price_per_kg,

    -- Calculated fields
    ISNULL(weighbridge_net_weight_kg, 0)
        - ISNULL(rotten_damaged_weight_kg, 0) AS acceptable_weight_kg,

    CASE
        WHEN ISNULL(weighbridge_net_weight_kg, 0) > 0
        THEN (
            (ISNULL(weighbridge_net_weight_kg, 0)
             - ISNULL(rotten_damaged_weight_kg, 0)) * 100.0
            / NULLIF(weighbridge_net_weight_kg, 0)
        )
        ELSE NULL
    END AS acceptable_pct

INTO #calc
FROM dbo.fact_tomato_intake
WHERE CAST(intake_date AS date) = @ReportDate;

-- =============================================
-- 1. DAILY SUMMARY
-- =============================================
SELECT
    @ReportDate AS report_date,
    COUNT(*) AS total_loads_received,
    SUM(ISNULL(weighbridge_net_weight_kg, 0)) AS total_net_weight_kg,
    SUM(ISNULL(usd_total_value, 0)) AS total_value_usd,
    AVG(price_per_kg) AS avg_price_per_kg,
    AVG(good_quality_brix) AS avg_brix,
    AVG(good_quality_ph) AS avg_ph
FROM #calc;

-- =============================================
-- 2. QUALITY / VALUE BREAKDOWN
-- =============================================
SELECT
    @ReportDate AS report_date,
    SUM(ISNULL(acceptable_weight_kg, 0)) AS acceptable_weight_kg,
    SUM(ISNULL(rotten_damaged_weight_kg, 0)) AS rotten_damaged_weight_kg,
    SUM(ISNULL(weighbridge_net_weight_kg, 0)) AS total_weight_kg,

    CASE
        WHEN SUM(ISNULL(weighbridge_net_weight_kg, 0)) > 0
        THEN SUM(ISNULL(acceptable_weight_kg, 0)) * 100.0
             / NULLIF(SUM(ISNULL(weighbridge_net_weight_kg, 0)), 0)
        ELSE NULL
    END AS acceptable_pct,

    CASE
        WHEN SUM(ISNULL(weighbridge_net_weight_kg, 0)) > 0
        THEN SUM(ISNULL(rotten_damaged_weight_kg, 0)) * 100.0
             / NULLIF(SUM(ISNULL(weighbridge_net_weight_kg, 0)), 0)
        ELSE NULL
    END AS rotten_damaged_pct,

    SUM(ISNULL(usd_total_value, 0)) AS total_value_usd
FROM #calc;

-- =============================================
-- 3. EXCEPTION COUNTS
-- =============================================
SELECT
    @ReportDate AS report_date,
    SUM(CASE WHEN rotten_damaged_pct >= 10 THEN 1 ELSE 0 END) AS high_rotten_loads,
    SUM(CASE WHEN good_quality_brix < 12 THEN 1 ELSE 0 END) AS low_brix_loads,
    SUM(CASE WHEN weighbridge_net_weight_kg IS NULL THEN 1 ELSE 0 END) AS missing_net_weight,
    SUM(CASE WHEN usd_total_value IS NULL THEN 1 ELSE 0 END) AS missing_total_value
FROM #calc;

-- =============================================
-- 4. EXCEPTION DETAIL
-- =============================================
SELECT
    @ReportDate AS report_date,
    submission_id,
    submission_number,
    submission_name,
    load_number,
    grower_name,
    farm_name,
    weighbridge_net_weight_kg,
    rotten_damaged_weight_kg,
    rotten_damaged_pct,
    good_quality_brix,
    good_quality_ph,
    usd_total_value,

    CASE
        WHEN rotten_damaged_pct >= 10 THEN 'High rotten %'
        WHEN good_quality_brix < 12 THEN 'Low brix'
        WHEN weighbridge_net_weight_kg IS NULL THEN 'Missing net weight'
        WHEN usd_total_value IS NULL THEN 'Missing total value'
        ELSE 'Check'
    END AS exception_reason

FROM #calc
WHERE
    rotten_damaged_pct >= 10
    OR good_quality_brix < 12
    OR weighbridge_net_weight_kg IS NULL
    OR usd_total_value IS NULL
ORDER BY submission_number;