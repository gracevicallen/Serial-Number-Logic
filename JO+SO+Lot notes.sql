WITH base AS (
    -- Start with the raw inventory transactions table (intran)
    -- and bring in a few helpful fields from item master (inmast).
    -- We are not using every inmast field yet, but this mirrors the original Domo source setup.
    SELECT
        intran.*,
        inmast.f2totcost AS item_master_cost,
        inmast.fcpurchase AS purchased,
        inmast.fgroup AS group_code
    FROM intran WITH (NOLOCK)
    LEFT JOIN inmast WITH (NOLOCK)
        -- Join item master by part number + part revision
        ON intran.fpartno = inmast.fpartno
       AND intran.fcpartrev = inmast.frev
    WHERE intran.fdate >= '2018-01-01'
        -- Limit data to transactions from 1/1/2018 forward
),

formula_tile_1 AS (
    -- Replicates the first formula tile from Domo.
    -- This creates cleaned/usable Job and Lot/SN fields,
    -- and also trims the part fields for consistency.
    SELECT
        *,

        -- Job:
        -- Use ftojob first if it exists and is not blank.
        -- Otherwise fall back to ffromjob.
        COALESCE(NULLIF(LTRIM(RTRIM(ftojob)), ''), LTRIM(RTRIM(ffromjob))) AS Job,

        -- Lot/SN:
        -- Use ffromlot first if it exists and is not blank.
        -- Otherwise fall back to ftolot.
        COALESCE(NULLIF(LTRIM(RTRIM(ffromlot)), ''), LTRIM(RTRIM(ftolot))) AS [Lot/SN],

        -- Trim spaces off part number and part rev
        LTRIM(RTRIM(fpartno)) AS [Part #],
        LTRIM(RTRIM(fcpartrev)) AS [Part rev],

        -- Rename fdate to a cleaner name
        fdate AS [Date]
    FROM base
),

select_columns_1 AS (
    -- This matches the Domo select-columns tile.
    -- At this point, we only keep the fields needed for the logic downstream.
    SELECT
        Job,
        [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM formula_tile_1
),

remove_duplicates_1 AS (
    -- Remove exact duplicate rows across all 6 selected columns.
    -- This does NOT yet decide "latest" anything.
    -- It just removes true duplicate transaction records.
    SELECT DISTINCT
        Job,
        [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM select_columns_1
),

string_ops_1 AS (
    -- Trim Job and Lot/SN again.
    -- This is defensive cleanup to ensure whitespace does not cause false mismatches.
    SELECT
        LTRIM(RTRIM(Job)) AS Job,
        LTRIM(RTRIM([Lot/SN])) AS [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM remove_duplicates_1
),

filter_rows_1 AS (
    -- Remove any rows where Job or Lot/SN is blank/null.
    -- These rows are not useful for assignment tracking.
    SELECT
        Job,
        [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM string_ops_1
    WHERE
        NULLIF(Job, '') IS NOT NULL
        AND NULLIF([Lot/SN], '') IS NOT NULL
),

most_recent_per_lot AS (
    -- For each Lot/SN, find the latest transaction timestamp.
    -- This tells us the most recent inventory movement for each lot.
    SELECT
        [Lot/SN],
        MAX(fctime_ts) AS [Most recent Date]
    FROM filter_rows_1
    GROUP BY [Lot/SN]
),

join_data_1 AS (
    -- Join the latest timestamp per lot back to the detailed rows.
    -- Result: keep only the row(s) that represent the current/latest state of each lot.
    SELECT
        d.Job,
        d.[Lot/SN],
        d.[Part #],
        d.[Part rev],
        d.[Date],
        d.fctime_ts
    FROM filter_rows_1 d
    INNER JOIN most_recent_per_lot g
        ON d.[Lot/SN] = g.[Lot/SN]
       AND d.fctime_ts = g.[Most recent Date]
),

most_recent_per_job_part AS (
    -- Now that each lot is reduced to its latest state,
    -- find the latest timestamp for each Job + Part # combination.
    --
    -- Why?
    -- A job can have multiple lots assigned/unassigned over time for the same part.
    -- We only want the current/latest lot assignment for each job+part.
    SELECT
        Job,
        [Part #],
        MAX(fctime_ts) AS [X2 MOST RECENT DATE]
    FROM join_data_1
    GROUP BY
        Job,
        [Part #]
),

join_data_2 AS (
    -- Join the latest Job + Part # timestamp back to the lot-level detail rows.
    -- Result: keep only the current/latest lot rows for each Job + Part #.
    SELECT
        d.Job,
        d.[Lot/SN],
        d.[Part #],
        d.[Part rev],
        d.[Date],
        d.fctime_ts
    FROM join_data_1 d
    INNER JOIN most_recent_per_job_part g
        ON d.Job = g.Job
       AND d.[Part #] = g.[Part #]
       AND d.fctime_ts = g.[X2 MOST RECENT DATE]
),

inventory_prepped AS (
    -- Final cleanup of the inventory-derived dataset before joining to shipping data.
    -- Trims Lot/SN one more time to make sure the join to shlotc works cleanly.
    SELECT
        Job,
        LTRIM(RTRIM([Lot/SN])) AS [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM join_data_2
),

shlotc_prepped AS (
    -- Prepare the shlotc table for the join.
    -- Trim the lot field and keep only lot + shipper number.
    -- DISTINCT is used because shlotc may contain repeated lot/shipper combinations.
    SELECT DISTINCT
        LTRIM(RTRIM(fclot)) AS [Lot/SN],
        fcshipno
    FROM shlotc WITH (NOLOCK)
    WHERE NULLIF(LTRIM(RTRIM(fclot)), '') IS NOT NULL
        -- Ignore blank lots
),

shitem_prepped AS (
    -- Prepare the shitem table for the second join.
    -- Keep the shipper number and the first 6 characters of fsokey,
    -- which is being used as Sales Order.
    SELECT DISTINCT
        fshipno,
        LEFT(LTRIM(RTRIM(fsokey)), 6) AS [Sales Order]
    FROM shitem WITH (NOLOCK)
),

joined_shipping AS (
    -- First left join inventory to shlotc by Lot/SN
    -- to pull in fcshipno (Shipper).
    -- Then left join to shitem by shipper number
    -- to pull in Sales Order.
    --
    -- LEFT JOIN is used so we do not lose any inventory rows
    -- if no shipping match exists.
    SELECT
        i.Job,
        i.[Lot/SN],
        si.[Sales Order],
        s.fcshipno AS Shipper,
        i.[Part #],
        i.[Part rev],
        i.[Date],
        i.fctime_ts
    FROM inventory_prepped i
    LEFT JOIN shlotc_prepped s
        ON i.[Lot/SN] = s.[Lot/SN]
    LEFT JOIN shitem_prepped si
        ON s.fcshipno = si.fshipno
),

remove_duplicates_2 AS (
    -- After the shipping joins, duplicates may appear because
    -- one lot may map to multiple shipping rows or one shipper
    -- may map to multiple shitem rows.
    --
    -- DISTINCT removes exact duplicate output rows.
    SELECT DISTINCT
        Job,
        [Lot/SN],
        [Sales Order],
        Shipper,
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM joined_shipping
)

-- Final output:
-- Return the organized columns in the order you wanted.
SELECT
    Job,
    [Lot/SN],
    [Sales Order],
    Shipper,
    [Part #],
    [Part rev],
    [Date],
    fctime_ts
FROM remove_duplicates_2;
