select * from 
datascience_public_misc.near_analytics.bridge_flows_daily; 

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on day-chain observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.bridge_flows_daily (
    day_ TIMESTAMP,
    other_chain VARCHAR,
    inbound_usd FLOAT,
    outbound_usd FLOAT,
    total_volume_usd FLOAT,
    net_flow_usd FLOAT,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering to optimize query performance 
ALTER TABLE datascience_public_misc.near_analytics.bridge_flows_daily
CLUSTER BY (day_, other_chain);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_bridge_flows_daily();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_bridge_flows_daily()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.bridge_flows_daily AS target
    USING (
        SELECT 
            DATE_TRUNC('day', block_timestamp) as day_,
            CASE 
                WHEN source_chain = 'near' THEN destination_chain
                WHEN destination_chain = 'near' THEN source_chain
            END as other_chain,
            SUM(CASE WHEN direction = 'inbound' THEN amount_usd ELSE 0 END) as inbound_usd,
            SUM(CASE WHEN direction = 'outbound' THEN amount_usd ELSE 0 END) as outbound_usd,
            SUM(amount_usd) as total_volume_usd,
            SUM(CASE 
                WHEN direction = 'inbound' THEN amount_usd 
                WHEN direction = 'outbound' THEN -amount_usd 
            END) as net_flow_usd
        FROM near.defi.ez_bridge_activity
        WHERE block_timestamp >= COALESCE(
            DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.bridge_flows_daily)),
            '1970-01-01' -- If table is empty, start from genesis
        )
        AND receipt_succeeded = true
        AND amount_usd IS NOT NULL
        AND other_chain IS NOT NULL
        AND other_chain NOT ILIKE '%null%'
        GROUP BY 1,2
    ) AS source
    ON target.day_ = source.day_ 
    AND target.other_chain = source.other_chain
    WHEN MATCHED THEN
        UPDATE SET 
            target.inbound_usd = source.inbound_usd,
            target.outbound_usd = source.outbound_usd,
            target.total_volume_usd = source.total_volume_usd,
            target.net_flow_usd = source.net_flow_usd,
            target._inserted_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            day_,
            other_chain,
            inbound_usd,
            outbound_usd,
            total_volume_usd,
            net_flow_usd,
            _inserted_timestamp
        )
        VALUES (
            source.day_,
            source.other_chain,
            source.inbound_usd,
            source.outbound_usd,
            source.total_volume_usd,
            source.net_flow_usd,
            CURRENT_TIMESTAMP()
        );

    RETURN 'Bridge flows daily metrics updated successfully';
END;
$$;

-- Create task to update bridge flows every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_bridge_flows_daily_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_bridge_flows_daily();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_bridge_flows_daily_task RESUME;

-- Step 5: Set appropriate permissions for Studio access
-- Grant schema-level permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.bridge_flows_daily TO ROLE INTERNAL_DEV;

-- Grant permissions for Velocity Ethereum Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.bridge_flows_daily TO ROLE VELOCITY_ETHEREUM;