
select 
day_, tx_count, user_count
 from datascience_public_misc.eclipse_analytics.daily_turbotap;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (day)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.daily_turbotap (
    day_ DATE,
    tx_count INTEGER,
    user_count INTEGER
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_daily_turbotap();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_daily_turbotap()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.eclipse_analytics.daily_turbotap AS target
    USING (
        SELECT 
            date_trunc('day', block_timestamp) as day_,
            count(distinct tx_id) as tx_count,
            count(distinct signers[0]) as user_count
        FROM eclipse.core.fact_events
        WHERE block_timestamp::date >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.eclipse_analytics.daily_turbotap)),
            '2024-01-01'
        )
        AND program_id = 'turboe9kMc3mSR8BosPkVzoHUfn5RVNzZhkrT2hdGxN'
        AND succeeded
        GROUP BY day_
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            target.tx_count = source.tx_count,
            target.user_count = source.user_count
    WHEN NOT MATCHED THEN
        INSERT (day_, tx_count, user_count)
        VALUES (source.day_, source.tx_count, source.user_count);

    RETURN 'Daily TurboTap metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.daily_turbotap
CLUSTER BY (day_);

-- Create task to update TurboTap metrics every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_daily_turbotap_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_daily_turbotap();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.eclipse_analytics.update_daily_turbotap_task RESUME;

-- Grant schema usage
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.daily_turbotap TO ROLE INTERNAL_DEV;

-- Grant individual access for Velocity Ethereum Studio
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.daily_turbotap TO ROLE VELOCITY_ETHEREUM;