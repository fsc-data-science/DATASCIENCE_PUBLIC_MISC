select * from datascience_public_misc.near_analytics.near_daily_storage_deposits
order by day_ desc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (chain-day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_daily_storage_deposits (
    day_ DATE,
    storage_amount_near FLOAT,
    _inserted_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _modified_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_near_daily_storage_deposits();

-- Step 4: Define the procedure with an intelligent lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_near_daily_storage_deposits()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.near_daily_storage_deposits AS target
    USING (
        SELECT
            DATE_TRUNC('day', block_timestamp) AS day_,
            SUM(DIV0(deposit, 1e24)) AS storage_amount_near
        FROM near.core.fact_actions_events_function_call
        WHERE method_name = 'storage_deposit'
        AND RECEIPT_SUCCEEDED = 'TRUE'
        AND block_timestamp >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_storage_deposits)),
            '1970-01-01'
        )
        GROUP BY 1
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET
            target.storage_amount_near = source.storage_amount_near,
            target._modified_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (day_, storage_amount_near, _inserted_timestamp, _modified_timestamp)
        VALUES (source.day_, source.storage_amount_near, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    RETURN 'NEAR daily storage deposits updated successfully';
END;
$$;

-- Step 5: Add clustering to the table
ALTER TABLE datascience_public_misc.near_analytics.near_daily_storage_deposits
CLUSTER BY (day_);

-- Step 6: Create task to update storage deposits every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_storage_deposits_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_near_daily_storage_deposits();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_near_daily_storage_deposits_task RESUME;

-- Step 7: Set appropriate permissions
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE INTERNAL_DEV;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_storage_deposits TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_storage_deposits TO ROLE VELOCITY_ETHEREUM;