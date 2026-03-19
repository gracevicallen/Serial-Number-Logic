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
select_columns_2 AS (
    SELECT
        Job,
        [Lot/SN],
        [Part #],
        [Part rev],
        [Date],
        fctime_ts
    FROM filter_rows_1
),
most_recent_per_lot AS (
    SELECT
        [Lot/SN],
        MAX(fctime_ts) AS [Most recent Date]
    FROM select_columns_2
    GROUP BY [Lot/SN]
),
join_data_1 AS (
    SELECT
        d.Job,
        d.[Lot/SN],
        d.[Part #],
        d.[Part rev],
        d.[Date],
        d.fctime_ts,
        g.[Most recent Date]
    FROM select_columns_2 d
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
)
SELECT *
FROM join_data_2;
