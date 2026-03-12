


-- ============================================================
-- STEP 1 — RAW IMPORT VIEW
--   Treat the CSV table exactly as-is so every
--   downstream CTE has a single source of truth.
-- ============================================================
WITH raw_data AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,
        Region,
        Category,
        Qty,
        Price,
        Discount
    FROM global_retailer_2026        -- ← replace with your actual table / import name
),


-- ============================================================
-- STEP 2 — DEDUPLICATE
--   Assign a row number partitioned by Transaction_ID.
--   Only row_num = 1 (the first occurrence) is kept in the
--   next CTE, which removes all ~50 duplicate IDs cleanly.
-- ============================================================
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Transaction_ID
            ORDER BY    Date            -- keeps the earliest-dated record
        ) AS row_num
    FROM raw_data
),

deduplicated AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,
        Region,
        Category,
        Qty,
        Price,
        Discount
    FROM ranked
    WHERE row_num = 1
),


-- ============================================================
-- STEP 3 — FIX NEGATIVE PRICES
--   ABS() converts any negative price entry to its absolute
--   value.  No legitimate retail price should be negative;
--   those rows are assumed to be data-entry sign errors.
-- ============================================================
fixed_prices AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,
        Region,
        Category,
        Qty,
        ABS(Price)  AS Price,          -- ← corrects ~34 negative rows
        Discount
    FROM deduplicated
),


-- ============================================================
-- STEP 4 — FILL MISSING QTY
--   COALESCE returns the first non-NULL argument.
--   Any NULL Qty (the ~74 blank cells) defaults to 1,
--   representing a single-unit purchase assumption.
-- ============================================================
filled_qty AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,
        Region,
        Category,
        COALESCE(Qty, 1)  AS Qty,      -- ← fills ~74 missing values
        Price,
        Discount
    FROM fixed_prices
),


-- ============================================================
-- STEP 5 — STANDARDISE REGION TO PROPER CASE
--   UPPER() + LOWER() slice lets us rebuild the string as
--   Proper Case without relying on dialect-specific functions
--   like INITCAP() (PostgreSQL / Oracle) so the logic is
--   portable.  Swap for INITCAP(Region) where supported.
--
--   Handles all observed variants:
--     'east' / 'EAST' / 'East'  → 'East'
--     'west' / 'WEST' / 'West'  → 'West'
--     'north'/ 'NORTH'/ 'North' → 'North'
--     'south'/ 'SOUTH'/ 'South' → 'South'
-- ============================================================
standardised_region AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,

        -- Portable Proper-Case via CASE (works on every engine)
        CASE UPPER(Region)
            WHEN 'EAST'  THEN 'East'
            WHEN 'WEST'  THEN 'West'
            WHEN 'NORTH' THEN 'North'
            WHEN 'SOUTH' THEN 'South'
            ELSE UPPER(SUBSTRING(Region, 1, 1))
                 || LOWER(SUBSTRING(Region, 2))   -- fallback for unexpected values
        END                           AS Region,

        -- Apply the same Proper-Case logic to Category
        CASE UPPER(Category)
            WHEN 'LAPTOPS'      THEN 'Laptops'
            WHEN 'PHONES'       THEN 'Phones'
            WHEN 'ACCESSORIES'  THEN 'Accessories'
            ELSE UPPER(SUBSTRING(Category, 1, 1))
                 || LOWER(SUBSTRING(Category, 2))
        END                           AS Category,

        Qty,
        Price,
        Discount
    FROM filled_qty
),


-- ============================================================
-- STEP 6 — CALCULATE TOTAL REVENUE
--   Total_Revenue = Qty × Price (post-ABS, post-COALESCE).
--   Net_Revenue additionally applies the Discount percentage,
--   giving a second useful metric for dashboards.
-- ============================================================
revenue_calc AS (
    SELECT
        Transaction_ID,
        Date,
        Customer_ID,
        Region,
        Category,
        Qty,
        Price,
        Discount,
        ROUND(Qty * Price, 2)                          AS Total_Revenue,
        ROUND(Qty * Price * (1 - Discount), 2)         AS Net_Revenue
    FROM standardised_region
),


-- ============================================================
-- STEP 7 — WINDOW FUNCTION ENRICHMENT
--   Add analytical columns that are useful for portfolio
--   dashboards or further analysis without any GROUP BY
--   aggregation (rows stay at transaction grain).
--
--   • region_revenue_rank : ranks each transaction by revenue
--                           within its Region
--   • category_avg_price  : average clean price per Category
--                           shown alongside every row
--   • running_total       : cumulative revenue ordered by Date
-- ============================================================
enriched AS (
    SELECT
        *,

        -- Rank transactions by Total_Revenue within each Region
        RANK() OVER (
            PARTITION BY Region
            ORDER BY     Total_Revenue DESC
        )                                              AS region_revenue_rank,

        -- Average price per Category across all clean rows
        ROUND(AVG(Price) OVER (
            PARTITION BY Category
        ), 2)                                          AS category_avg_price,

        -- Running cumulative revenue ordered chronologically
        ROUND(SUM(Total_Revenue) OVER (
            ORDER BY     Date, Transaction_ID          -- tie-break for same-day rows
            ROWS BETWEEN UNBOUNDED PRECEDING
                     AND CURRENT ROW
        ), 2)                                          AS running_total_revenue

    FROM revenue_calc
)


-- ============================================================
-- FINAL SELECT — Clean, enriched, analysis-ready dataset
-- ============================================================
SELECT
    Transaction_ID,
    Date,
    Customer_ID,
    Region,
    Category,
    Qty,
    Price,
    Discount,
    Total_Revenue,
    Net_Revenue,
    region_revenue_rank,
    category_avg_price,
    running_total_revenue
FROM enriched
ORDER BY Date, Transaction_ID;