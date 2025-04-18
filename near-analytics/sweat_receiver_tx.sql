select *
from datascience_public_misc.near_analytics.sweat_welcome_tx_stats
order by day_ asc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on dayly observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_welcome_tx_stats (
    day_ TIMESTAMP,
    n_tx_lifetime INTEGER,
    n_tx_post_receive INTEGER,
    cumulative_tx INTEGER,
    cumulative_tx_post_receive INTEGER,
    total_near_received FLOAT,
    cumulative_dispersed FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_sweat_welcome_tx_stats();

-- Step 4: Define the procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_sweat_welcome_tx_stats()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate existing table
    TRUNCATE TABLE datascience_public_misc.near_analytics.sweat_welcome_tx_stats;
    
    -- Insert full historical data
    INSERT INTO datascience_public_misc.near_analytics.sweat_welcome_tx_stats
    WITH distributions AS (
        SELECT 
            DATE_TRUNC('day', block_timestamp) AS day_,
            SUM(amount) AS total_near_received
        FROM datascience_public_misc.near_analytics.sweat_welcome_transfers
        GROUP BY day_
    ),

    first_transfers AS (
        SELECT 
            sweat_receiver,
            MIN(block_timestamp) AS first_transfer_ts
        FROM datascience_public_misc.near_analytics.sweat_welcome_transfers
        GROUP BY sweat_receiver
    ),

    dayly_tx_lifetime AS (
        SELECT 
            DATE_TRUNC('day', t.block_timestamp) AS day_,
            COUNT(t.tx_hash) AS n_tx_lifetime
        FROM 
            near.core.fact_transactions t 
            INNER JOIN first_transfers w 
                ON t.tx_signer = w.sweat_receiver 
        GROUP BY day_ 
    ),

    dayly_post_receive_tx AS (
        SELECT 
            DATE_TRUNC('day', t.block_timestamp) AS day_,
            COUNT(t.tx_hash) AS n_tx_post_receive
        FROM 
            near.core.fact_transactions t 
            INNER JOIN first_transfers w 
                ON t.tx_signer = w.sweat_receiver 
                AND t.block_timestamp > w.first_transfer_ts
        GROUP BY day_ 
    ),

    dayly_metrics AS (
        SELECT 
            COALESCE(t.day_, d.day_) AS day_,  -- Use tx days as our base
            COALESCE(t.n_tx_lifetime, 0) AS n_tx_lifetime,
            COALESCE(p.n_tx_post_receive, 0) AS n_tx_post_receive,
            COALESCE(d.total_near_received, 0) AS total_near_received
        FROM dayly_tx_lifetime t  -- This has all days where there were transactions
        FULL JOIN distributions d ON t.day_ = d.day_
        LEFT JOIN dayly_post_receive_tx p ON t.day_ = p.day_
    )
    

    SELECT 
        day_,
        n_tx_lifetime,
        n_tx_post_receive,
        SUM(n_tx_lifetime) OVER (ORDER BY day_) AS cumulative_tx,
        SUM(n_tx_post_receive) OVER (ORDER BY day_) AS cumulative_tx_post_receive,
        total_near_received,
        SUM(total_near_received) OVER (ORDER BY day_) AS cumulative_dispersed
    FROM dayly_metrics
    ORDER BY day_;


    RETURN 'SWEAT welcome transaction stats updated successfully';
END;
$$;

-- Create task to update stats every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_sweat_welcome_tx_stats_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_sweat_welcome_tx_stats();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_sweat_welcome_tx_stats_task RESUME;


-- Set appropriate permissions for Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.sweat_welcome_tx_stats TO ROLE VELOCITY_ETHEREUM;

-- Grant permissions to INTERNAL_DEV role
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_welcome_tx_stats TO ROLE INTERNAL_DEV;