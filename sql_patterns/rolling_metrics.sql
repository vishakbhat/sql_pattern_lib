-- =============================================================================
-- PATTERN: Rolling Window Metrics (7-day and 30-day)
-- =============================================================================
-- Purpose:
--   Compute rolling aggregates over a time series using window functions.
--   Demonstrates the difference between a standard GROUP BY aggregate
--   (which collapses rows) and a window function (which preserves row grain).
--
-- Pattern use cases:
--   - Rolling average payment amounts per customer
--   - Rolling default event count over trailing periods
--   - Rolling revenue over trailing days (retail)
--   - Any metric that needs a "trailing N days" view per entity
--
-- Grain:
--   One row per (customer_id, payment_date).
--   The rolling metrics are ADDITIONAL columns on this grain —
--   they do not change the grain of the result set.
--
-- Key concepts demonstrated:
--   1. ROWS BETWEEN frame vs RANGE BETWEEN frame
--   2. Handling NULLs in rolling windows (early rows with fewer than N periods)
--   3. Partitioning by entity (customer) before applying the window
--   4. Separating spine CTE from metric CTEs for readability
--   5. Inline assertions to document expected behaviour
--
-- Dataset context (P1 - Credit Risk):
--   Source table: stg_payments
--   Columns used: customer_id, payment_date, payment_amount, is_late_payment
--
-- Dependencies:
--   stg_payments must exist and be populated before running this script.
--   Run: scripts/build_staging.sql first.
--
-- Author:  Vishak Bhat
-- Created: 2026-03-02
-- Version: 1.0
-- =============================================================================


-- =============================================================================
-- SECTION 1: Sample data setup
-- Use this block to run the pattern standalone without the full staging layer.
-- Drop and recreate a minimal table that mimics stg_payments structure.
-- Comment this section out when running against the real staging table.
-- =============================================================================

DROP TABLE IF EXISTS stg_payments;

CREATE TABLE stg_payments (
    customer_id     INTEGER       NOT NULL,
    payment_date    DATE          NOT NULL,
    payment_amount  NUMERIC(10,2) NOT NULL,
    is_late_payment BOOLEAN       NOT NULL  -- TRUE if payment was overdue
);

-- Insert sample rows covering 3 customers over ~45 days.
-- Edge cases included:
--   - Customer 1 has a gap (no payment on some days) to test window behaviour
--   - Customer 2 has a late payment flag to test boolean aggregation
--   - Customer 3 has only 5 rows (fewer than the 30-day window) to show NULL behaviour

INSERT INTO stg_payments VALUES
    -- Customer 1: regular payments, no late flags
    (1, '2026-01-01', 500.00, FALSE),
    (1, '2026-01-08', 450.00, FALSE),
    (1, '2026-01-15', 600.00, FALSE),
    (1, '2026-01-22', 480.00, FALSE),
    (1, '2026-01-29', 520.00, FALSE),
    (1, '2026-02-05', 510.00, FALSE),
    (1, '2026-02-12', 490.00, FALSE),

    -- Customer 2: mix of on-time and late payments
    (2, '2026-01-03', 300.00, FALSE),
    (2, '2026-01-10', 280.00, TRUE),   -- late
    (2, '2026-01-17', 310.00, FALSE),
    (2, '2026-01-24', 290.00, TRUE),   -- late
    (2, '2026-01-31', 305.00, FALSE),
    (2, '2026-02-07', 295.00, TRUE),   -- late
    (2, '2026-02-14', 315.00, FALSE),

    -- Customer 3: only 5 payments (tests rolling window with insufficient history)
    (3, '2026-01-20', 700.00, FALSE),
    (3, '2026-01-27', 680.00, FALSE),
    (3, '2026-02-03', 720.00, FALSE),
    (3, '2026-02-10', 710.00, FALSE),
    (3, '2026-02-17', 695.00, FALSE);


-- =============================================================================
-- SECTION 2: Core pattern query
-- =============================================================================

-- ------------------------------------------------------------
-- CTE 1: spine
-- Purpose: Establishes the base grain and ordering.
--   Always define ORDER BY inside the window function, not here,
--   unless you need a guaranteed output order at the end.
--   The spine CTE keeps the base query clean and easy to audit.
-- ------------------------------------------------------------
WITH spine AS (
    SELECT
        customer_id,
        payment_date,
        payment_amount,
        is_late_payment
    FROM stg_payments
),

-- ------------------------------------------------------------
-- CTE 2: rolling_metrics
-- Purpose: Adds rolling aggregates as new columns on the same grain.
--
-- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
--   = the current row + the 6 rows before it (7 rows total) ordered by date.
--   This is a ROW-based frame — it counts rows, not calendar days.
--   Use this when your data has consistent row spacing (e.g. one row per week).
--
--   WARNING: If rows are not evenly spaced by date, use a date-spine approach
--   instead. See pattern: date_spine_with_gaps.sql for that approach.
--
-- ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
--   = current row + 29 rows before = 30-row rolling window.
--
-- PARTITION BY customer_id
--   Resets the window for each customer. Without this, the window
--   would roll across customers — a silent join trap.
-- ------------------------------------------------------------
rolling_metrics AS (
    SELECT
        customer_id,
        payment_date,
        payment_amount,
        is_late_payment,

        -- ── 7-row rolling metrics ─────────────────────────────────────────
        -- Average payment amount over the trailing 7 rows for this customer.
        -- NULLs will not appear here because AVG ignores NULLs, but if a
        -- customer has fewer than 7 prior rows the average uses what exists.
        AVG(payment_amount) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7_avg_payment,

        -- Count of late payments in trailing 7 rows.
        -- Casting boolean to integer (1/0) allows SUM to act as a count.
        SUM(is_late_payment::INTEGER) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7_late_count,

        -- ── 30-row rolling metrics ────────────────────────────────────────
        -- Same pattern extended to 30 rows.
        -- For customers with fewer than 30 payments, this uses all available
        -- rows — it does NOT return NULL. Use COUNT(*) OVER the same window
        -- to know how many rows contributed (see rolling_30_row_count below).
        AVG(payment_amount) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30_avg_payment,

        SUM(is_late_payment::INTEGER) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30_late_count,

        -- ── Diagnostic columns ────────────────────────────────────────────
        -- Row count within the window — tells you whether the window is
        -- "full" (30 rows) or "partial" (early rows for a new customer).
        -- Essential for distinguishing a true low average from an artifact
        -- of insufficient history.
        COUNT(*) OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30_row_count,

        -- Row number within customer partition ordered by date.
        -- Used for filtering (e.g. "get the first payment row per customer")
        -- and for debugging window behaviour.
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY payment_date
        ) AS customer_payment_seq

    FROM spine
),

-- ------------------------------------------------------------
-- CTE 3: final
-- Purpose: Adds derived flags and rounds values for presentation.
--   Separating this from rolling_metrics keeps the window function
--   CTE clean and avoids nesting CASE inside window expressions.
-- ------------------------------------------------------------
final AS (
    SELECT
        customer_id,
        payment_date,
        payment_amount,
        is_late_payment,

        -- Round rolling averages to 2 decimal places for readability.
        ROUND(rolling_7_avg_payment, 2)  AS rolling_7_avg_payment,
        ROUND(rolling_30_avg_payment, 2) AS rolling_30_avg_payment,

        rolling_7_late_count,
        rolling_30_late_count,
        rolling_30_row_count,
        customer_payment_seq,

        -- Flag: is the 30-day window "full" (all 30 rows available)?
        -- Use this to filter out early rows when computing portfolio-level
        -- summaries — averaging partial windows distorts results.
        CASE
            WHEN rolling_30_row_count = 30 THEN TRUE
            ELSE FALSE
        END AS is_full_30_window,

        -- Flag: rolling late payment rate above threshold.
        -- Threshold: more than 1 late payment in trailing 7 rows.
        -- Adjust threshold based on business rule agreed with stakeholder.
        CASE
            WHEN rolling_7_late_count > 1 THEN TRUE
            ELSE FALSE
        END AS is_elevated_late_risk

    FROM rolling_metrics
)

-- =============================================================================
-- SECTION 3: Output
-- =============================================================================
SELECT
    customer_id,
    payment_date,
    payment_amount,
    is_late_payment,
    customer_payment_seq,
    rolling_7_avg_payment,
    rolling_7_late_count,
    rolling_30_avg_payment,
    rolling_30_late_count,
    rolling_30_row_count,
    is_full_30_window,
    is_elevated_late_risk
FROM final
ORDER BY
    customer_id,
    payment_date;


-- =============================================================================
-- SECTION 4: Validation spot-checks
-- Run these after the main query to verify expected behaviour.
-- In a dbt project these become singular tests under tests/.
-- =============================================================================

-- Check 1: Row count matches source.
-- Expected: same number of rows as stg_payments (no fan-out, no drops).
SELECT
    'row_count_check'                   AS check_name,
    COUNT(*)                            AS result_rows,
    (SELECT COUNT(*) FROM stg_payments) AS source_rows,
    CASE
        WHEN COUNT(*) = (SELECT COUNT(*) FROM stg_payments)
        THEN 'PASS' ELSE 'FAIL'
    END AS status
FROM final;


-- Check 2: First row per customer always has rolling_30_row_count = 1.
-- Verifies window frame starts correctly at sequence position 1.
SELECT
    'first_row_window_size_check' AS check_name,
    COUNT(*)                      AS violations
FROM final
WHERE customer_payment_seq = 1
  AND rolling_30_row_count  <> 1;
-- Expected: 0 violations


-- Check 3: rolling_7_late_count never exceeds 7.
-- A value > 7 would indicate the frame is wider than defined.
SELECT
    'late_count_ceiling_check' AS check_name,
    COUNT(*)                   AS violations
FROM final
WHERE rolling_7_late_count > 7;
-- Expected: 0 violations


-- Check 4: rolling_30_row_count never exceeds 30.
SELECT
    'row_count_ceiling_check' AS check_name,
    COUNT(*)                  AS violations
FROM final
WHERE rolling_30_row_count > 30;
-- Expected: 0 violations


-- =============================================================================
-- SECTION 5: Pattern notes and common pitfalls
-- =============================================================================
--
-- PITFALL 1: ROWS vs RANGE framing
--   ROWS BETWEEN N PRECEDING AND CURRENT ROW  → counts N physical rows
--   RANGE BETWEEN N PRECEDING AND CURRENT ROW → counts rows within a value range
--   For date-ordered data, ROWS is almost always what you want.
--   RANGE can produce unexpected results with duplicate dates.
--
-- PITFALL 2: Missing PARTITION BY
--   Omitting PARTITION BY rolls the window across ALL rows in the table.
--   Always include PARTITION BY for per-entity rolling metrics.
--
-- PITFALL 3: Averaging partial windows
--   For early rows (insufficient history), AVG uses what exists — it does
--   NOT return NULL or zero. This can inflate or deflate portfolio-level
--   averages. Always filter using is_full_30_window = TRUE when computing
--   downstream aggregates that require a full window.
--
-- PITFALL 4: Boolean aggregation in Postgres
--   Postgres does not support SUM(boolean). Cast to integer first:
--   SUM(col::INTEGER) or SUM(col::INT).
--   In Databricks/Spark SQL use CAST(col AS INT) or IF(col, 1, 0).
--
-- PITFALL 5: ORDER BY inside the window vs ORDER BY in the outer query
--   ORDER BY inside OVER() defines the window frame ordering.
--   ORDER BY at the end of the query defines output ordering.
--   They are independent. Always specify both explicitly.
--
-- =============================================================================
-- Related patterns:
--   sql_patterns/latest_record_per_key.sql   → deduplication using ROW_NUMBER
--   sql_patterns/percent_of_total.sql        → SUM OVER without ORDER BY
--   sql_patterns/date_spine_with_gaps.sql    → calendar-day rolling (not row-based)
--   sql_patterns/lag_lead_deltas.sql         → period-over-period change
-- =============================================================================
