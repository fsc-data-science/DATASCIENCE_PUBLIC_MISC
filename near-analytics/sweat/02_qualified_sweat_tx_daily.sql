select * from datascience_public_misc.near_analytics.qualified_user_transactions
order by day_ desc
limit 10;


-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on daily observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.qualified_user_transactions (
    day_ DATE,
    n_direct_tx INTEGER,
    n_relay_tx INTEGER,
    total_tx INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_qualified_user_transactions();

-- Step 4: Define the procedure with a 2-day lookback
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_qualified_user_transactions()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.qualified_user_transactions AS target
    USING (
        WITH daily_direct_sign AS ( 
            SELECT 
                DATE_TRUNC('day', t.block_timestamp) AS day_,
                COUNT(t.tx_hash) AS n_direct_tx
            FROM 
                near.core.fact_transactions t 
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users qsu 
                ON t.tx_signer = qsu.sweat_receiver
            WHERE 
                qsu.is_first_sweat_receive = 1
                AND t.block_timestamp >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.qualified_user_transactions)),
                    '1970-01-01'
                )
            GROUP BY day_ 
        ),
        
        daily_relay AS ( 
            SELECT 
                DATE_TRUNC('day', t.block_timestamp) AS day_,
                COUNT(t.tx_hash) AS n_relay_tx
            FROM 
                near.core.fact_transactions t 
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users qsu 
                ON t.tx_receiver = qsu.sweat_receiver
            WHERE 
                qsu.is_first_sweat_receive = 1
                AND t.tx_signer = 'sweat-relayer.near'
                AND t.block_timestamp >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.qualified_user_transactions)),
                    '1970-01-01'
                )
            GROUP BY day_
        ),
        
        combined_metrics AS (
            SELECT 
                COALESCE(d.day_, r.day_) AS day_,
                COALESCE(d.n_direct_tx, 0) AS n_direct_tx,
                COALESCE(r.n_relay_tx, 0) AS n_relay_tx,
                COALESCE(d.n_direct_tx, 0) + COALESCE(r.n_relay_tx, 0) AS total_tx
            FROM daily_direct_sign d
            FULL JOIN daily_relay r ON d.day_ = r.day_
        )
        
        SELECT * FROM combined_metrics
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            n_direct_tx = source.n_direct_tx,
            n_relay_tx = source.n_relay_tx,
            total_tx = source.total_tx
    WHEN NOT MATCHED THEN
        INSERT (day_, n_direct_tx, n_relay_tx, total_tx)
        VALUES (source.day_, source.n_direct_tx, source.n_relay_tx, source.total_tx);

    RETURN 'Qualified user transactions updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.qualified_user_transactions
CLUSTER BY (day_);

-- Create task to update metrics every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_qualified_user_transactions_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_qualified_user_transactions();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_qualified_user_transactions_task RESUME;

-- Set appropriate permissions for Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.qualified_user_transactions TO ROLE VELOCITY_ETHEREUM;

-- Grant permissions to INTERNAL_DEV role
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.qualified_user_transactions TO ROLE INTERNAL_DEV;