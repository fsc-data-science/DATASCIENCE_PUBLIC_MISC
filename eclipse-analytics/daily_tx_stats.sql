
select max(day_) from datascience_public_misc.eclipse_analytics.daily_tx_stats;


-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (day-level)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.daily_tx_stats (
    day_ DATE,
    n_tx INTEGER,
    n_succeeded INTEGER,
    n_blocks INTEGER,
    total_fees_eth FLOAT,
    median_fee_eth FLOAT,
    total_compute_units INTEGER,
    total_compute_limit INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_daily_tx_stats();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_daily_tx_stats()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.daily_tx_stats AS target
    USING (
        SELECT 
            DATE_TRUNC('day', block_timestamp) AS day_,
            COUNT(tx_id) AS n_tx,
            SUM(CASE WHEN succeeded THEN 1 ELSE 0 END) AS n_succeeded,
            COUNT(DISTINCT block_id) AS n_blocks,
            SUM(fee) / 1e9 AS total_fees_eth,
            MEDIAN(fee) / 1e9 AS median_fee_eth,
            SUM(units_consumed) AS total_compute_units,
            SUM(units_limit) AS total_compute_limit
        FROM eclipse.core.fact_transactions
        WHERE  block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.eclipse_analytics.daily_tx_stats)),
                '1970-01-01'
            )
        GROUP BY 1
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            n_tx = source.n_tx,
            n_succeeded = source.n_succeeded,
            n_blocks = source.n_blocks,
            total_fees_eth = source.total_fees_eth,
            median_fee_eth = source.median_fee_eth,
            total_compute_units = source.total_compute_units,
            total_compute_limit = source.total_compute_limit
    WHEN NOT MATCHED THEN
        INSERT (
            day_, n_tx, n_succeeded, n_blocks, total_fees_eth, 
            median_fee_eth, total_compute_units, total_compute_limit
        )
        VALUES (
            source.day_, source.n_tx, source.n_succeeded, source.n_blocks, 
            source.total_fees_eth, source.median_fee_eth, 
            source.total_compute_units, source.total_compute_limit
        );

    RETURN 'Eclipse daily transaction metrics updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.daily_tx_stats
CLUSTER BY (day_);


-- Create task to update daily signers every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_daily_tx_stats_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS CALL datascience_public_misc.eclipse_analytics.update_daily_tx_stats();

ALTER TASK datascience_public_misc.eclipse_analytics.update_daily_tx_stats_task RESUME;


-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.daily_tx_stats TO ROLE INTERNAL_DEV;

-- Individual access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.daily_tx_stats TO ROLE VELOCITY_ETHEREUM;