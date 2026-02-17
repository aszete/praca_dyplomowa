CREATE OR ALTER PROCEDURE gold.load_dim_time
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Ensure Table Exists
    IF OBJECT_ID('gold.dim_time') IS NULL
    BEGIN
        CREATE TABLE gold.dim_time (
            time_key INT PRIMARY KEY, -- Format: HHmm (e.g., 0830, 2215)
            time_24 VARCHAR(5),       -- Format: "08:30", "22:15"
            hour_24 INT,              -- 0 to 23
            minute INT,               -- 0 to 59
            time_of_day VARCHAR(20)   -- Marketing segments
        );
    END

    TRUNCATE TABLE gold.dim_time;

    -- 2. Populate 1440 minutes using a CTE
    WITH MinuteSeries AS (
        SELECT 0 AS n
        UNION ALL
        SELECT n + 1 FROM MinuteSeries WHERE n < 1439
    ),
    TimeList AS (
        SELECT DATEADD(MINUTE, n, CAST('00:00' AS TIME)) AS t
        FROM MinuteSeries
    )
    INSERT INTO gold.dim_time (time_key, time_24, hour_24, minute, time_of_day)
    SELECT 
        CAST(REPLACE(LEFT(CAST(t AS VARCHAR), 5), ':', '') AS INT),
        LEFT(CAST(t AS VARCHAR), 5),
        DATEPART(HOUR, t),
        DATEPART(MINUTE, t),
        CASE 
            WHEN DATEPART(HOUR, t) BETWEEN 0 AND 5 THEN 'Noc'          -- Night
            WHEN DATEPART(HOUR, t) BETWEEN 6 AND 11 THEN 'Rano'       -- Morning
            WHEN DATEPART(HOUR, t) BETWEEN 12 AND 17 THEN 'Popołudnie' -- Afternoon
            ELSE 'Wieczór'                                            -- Evening
        END
    FROM TimeList
    OPTION (MAXRECURSION 1440);

    PRINT 'Gold dim_time loaded successfully.';
END;
