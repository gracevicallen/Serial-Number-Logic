WITH base AS (
    SELECT
        intran.*,
        inmast.f2totcost AS item_master_cost,
        inmast.fcpurchase AS purchased,
        inmast.fgroup AS group_code
    FROM intran WITH (NOLOCK)
    LEFT JOIN inmast WITH (NOLOCK)
        ON intran.fpartno = inmast.fpartno
       AND intran.fcpartrev = inmast.frev
    WHERE intran.fdate >= '2018-01-01'
),
formula_tile_1 AS (
    SELECT
        *,
        COALESCE(NULLIF(LTRIM(RTRIM(ftojob)), ''), LTRIM(RTRIM(ffromjob))) AS Job,
        COALESCE(NULLIF(LTRIM(RTRIM(ffromlot)), ''), LTRIM(RTRIM(ftolot))) AS [Lot/SN],
        LTRIM(RTRIM(fpartno)) AS [Part #],
        LTRIM(RTRIM(fcpartrev)) AS [Part rev],
        fdate AS [Date]
    FROM base
),
select_columns_1 AS (
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
    SELECT
        [Lot/SN],
        MAX(fctime_ts) AS [Most recent Date]
    FROM filter_rows_1
    GROUP BY [Lot/SN]
),
join_data_1 AS (
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
    SELECT DISTINCT
        LTRIM(RTRIM(fclot)) AS [Lot/SN],
        fcshipno
    FROM shlotc WITH (NOLOCK)
    WHERE NULLIF(LTRIM(RTRIM(fclot)), '') IS NOT NULL
),
shitem_prepped AS (
    SELECT DISTINCT
        fshipno,
        LEFT(LTRIM(RTRIM(fsokey)), 6) AS [Sales Order]
    FROM shitem WITH (NOLOCK)
),
joined_shipping AS (
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
