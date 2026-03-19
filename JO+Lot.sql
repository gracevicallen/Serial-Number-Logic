WITH qalotc_base AS (
    SELECT
        LTRIM(RTRIM(fcuseindoc)) AS Job,
        LTRIM(RTRIM(fclot)) AS [Lot/SN],
        LTRIM(RTRIM(fcpartno)) AS [Part #],
        LTRIM(RTRIM(fcpartrev)) AS [Part rev],
        fddate AS [Date]
    FROM dbo.qalotc
    WHERE fcuseindoc IS NOT NULL
      AND LEN(LTRIM(RTRIM(fcuseindoc))) = 10
),
qalotc_clean AS (
    SELECT
        Job,
        [Lot/SN],
        [Part #],
        [Part rev],
        [Date]
    FROM qalotc_base
    WHERE NULLIF([Lot/SN], '') IS NOT NULL
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
        q.Job,
        q.[Lot/SN],
        si.[Sales Order],
        s.fcshipno AS Shipper,
        q.[Part #],
        q.[Part rev],
        q.[Date]
    FROM qalotc_clean q
    LEFT JOIN shlotc_prepped s
        ON q.[Lot/SN] = s.[Lot/SN]
    LEFT JOIN shitem_prepped si
        ON s.fcshipno = si.fshipno
),
remove_duplicates AS (
    SELECT DISTINCT
        Job,
        [Lot/SN],
        [Sales Order],
        Shipper,
        [Part #],
        [Part rev],
        [Date]
    FROM joined_shipping
)
SELECT
    Job,
    [Lot/SN],
    [Sales Order],
    Shipper,
    [Part #],
    [Part rev],
    [Date]
FROM remove_duplicates;
