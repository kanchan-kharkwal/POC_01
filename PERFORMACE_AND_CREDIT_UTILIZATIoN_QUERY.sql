-- PERFORMACE AND CREDIT UTILIZATION QUERY

-- TABLE (VISULIZATION)
CREATE OR REPLACE TABLE AKASX.PUBLIC.CREDIT_QUERY_MONITOR (
    query_id STRING,
    user_name STRING,
    execution_time FLOAT,
    wh_name STRING,
    hour_window TIMESTAMP,
    wh_credit FLOAT,
    query_credit FLOAT,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

select * from CREDIT_QUERY_MONITOR LIMIT 150;

-- INSERT QUERY 
INSERT INTO AKASX.PUBLIC.CREDIT_QUERY_MONITOR (
    query_id, user_name, execution_time, wh_name, hour_window, wh_credit, query_credit
)
WITH query_data AS (
    SELECT 
        q.query_id,
        q.execution_status,
        q.start_time,
        q.end_time,
        q.total_elapsed_time / 1000 AS elapsed_seconds,
        q.warehouse_name,
        q.user_name,
        DATEDIFF('hour', q.start_time, q.end_time) AS duration_hour,
        DATE_TRUNC('hour', q.start_time) AS hour_window
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
    WHERE q.start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP()) -- last 1 day
        AND q.warehouse_name IS NOT NULL
        AND q.execution_status = 'SUCCESS'
),
warehouse_credits AS (
    SELECT 
        warehouse_name,
        start_time AS hour_window,
        credits_used
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
),
joined AS (
    SELECT 
        q.query_id,
        q.user_name,
        q.start_time,
        q.end_time,
        q.elapsed_seconds,
        q.warehouse_name,
        q.hour_window,
        w.credits_used as wh_credit,
        -- Credit approximation: query time / total seconds in hour (3600)
        ROUND(((q.elapsed_seconds / 3600.0) * w.credits_used) * 1000, 6) AS estimated_query_credits
    FROM query_data q
    LEFT JOIN warehouse_credits w
      ON q.warehouse_name = w.warehouse_name AND q.hour_window = w.hour_window
)
SELECT 
        query_id,
        user_name,
        elapsed_seconds as execution_time,
        warehouse_name,
        hour_window,
        wh_credit,
        estimated_query_credits as query_credit
FROM joined
ORDER BY estimated_query_credits DESC;


