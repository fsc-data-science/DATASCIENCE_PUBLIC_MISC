select * from datascience_public_misc.near_analytics.sweat_relay_usage
limit 10;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_relay_usage (
    block_timestamp TIMESTAMP,
    block_id NUMBER,
    tx_hash VARCHAR,
    tx_signer VARCHAR,
    sweat_relay_user VARCHAR
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_sweat_relay_usage();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_sweat_relay_usage()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.sweat_relay_usage AS target
    USING (
        SELECT 
            block_timestamp,
            block_id,
            tx_hash,
            tx_signer,
            tx_receiver as sweat_relay_user
        FROM near.core.fact_transactions 
        WHERE tx_signer = 'sweat-relayer.near'
        AND tx_receiver NOT IN ('oracle.sweat', 'sweat-relayer.near', 'nomnomnom.testnet')
        AND block_timestamp >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(block_timestamp) FROM datascience_public_misc.near_analytics.sweat_relay_usage)),
            '1970-01-01'
        )
    ) AS source
    ON target.tx_hash = source.tx_hash
    WHEN MATCHED THEN
        UPDATE SET 
            target.block_timestamp = source.block_timestamp,
            target.block_id = source.block_id,
            target.tx_signer = source.tx_signer,
            target.sweat_relay_user = source.sweat_relay_user
    WHEN NOT MATCHED THEN
        INSERT (
            block_timestamp,
            block_id,
            tx_hash,
            tx_signer,
            sweat_relay_user
        )
        VALUES (
            source.block_timestamp,
            source.block_id,
            source.tx_hash,
            source.tx_signer,
            source.sweat_relay_user
        );

    RETURN 'SWEAT relay usage updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.sweat_relay_usage
CLUSTER BY (block_timestamp, sweat_relay_user);

-- Set appropriate permissions
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.sweat_relay_usage TO ROLE VELOCITY_ETHEREUM;

GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_relay_usage TO ROLE INTERNAL_DEV;

-- Create task to update sweat relay usage every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_sweat_relay_usage_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_sweat_relay_usage();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_sweat_relay_usage_task SUSPEND;