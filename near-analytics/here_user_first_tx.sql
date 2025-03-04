-- .tg first sign 

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.heretg_first_tx (
    direct_tg_signer VARCHAR,
    first_tx TIMESTAMP
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_heretg_first_tx();

-- Step 4: Define the procedure with an intelligent lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_heretg_first_tx()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
MERGE INTO datascience_public_misc.near_analytics.heretg_first_tx AS target
USING (
    SELECT 
        tx_signer AS direct_tg_signer,
        MIN(block_timestamp) AS first_tx
    FROM near.core.fact_transactions 
    WHERE
        tx_signer NOT IN ('relay.tg', '0-relay.hot.tg')
        AND ENDSWITH(tx_signer, '.tg')
        AND block_timestamp >= COALESCE(
            DATEADD(day, -7, (SELECT MIN(first_tx) FROM datascience_public_misc.near_analytics.heretg_first_tx)),
            '2020-01-01'
        )
    GROUP BY tx_signer
) AS source
ON target.direct_tg_signer = source.direct_tg_signer
WHEN MATCHED THEN
    UPDATE SET target.first_tx = 
        CASE 
            WHEN source.first_tx < target.first_tx THEN source.first_tx 
            ELSE target.first_tx 
        END
WHEN NOT MATCHED THEN
    INSERT (direct_tg_signer, first_tx)
    VALUES (source.direct_tg_signer, source.first_tx);

RETURN 'NEAR TG users first transaction data updated successfully';
END;
$$;

-- Add clustering to the table
ALTER TABLE datascience_public_misc.near_analytics.heretg_first_tx
CLUSTER BY (first_tx);

-- Create task to update heretg first tx every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_heretg_first_tx_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
  CALL datascience_public_misc.near_analytics.update_heretg_first_tx();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.near_analytics.update_heretg_first_tx_task RESUME;

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.heretg_first_tx TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.heretg_first_tx TO ROLE VELOCITY_ETHEREUM;