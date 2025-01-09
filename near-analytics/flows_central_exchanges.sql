select max(day_) 
from 
datascience_public_misc.near_analytics.central_exchange_flows
limit 10;



-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (day-exchange level)
CREATE TABLE IF NOT EXISTS datascience_public_misc.near_analytics.central_exchange_flows (
    day_ TIMESTAMP,
    exchange_name VARCHAR,
    inbound_volume_usd FLOAT,
    outbound_volume_usd FLOAT,
    net_volume_usd FLOAT,
    total_volume_usd FLOAT
);

-- Add clustering to optimize query performance
ALTER TABLE datascience_public_misc.near_analytics.central_exchange_flows
CLUSTER BY (day_, exchange_name);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_central_exchange_flows();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_central_exchange_flows()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.central_exchange_flows AS target
    USING (
        WITH cex_addresses AS (
            SELECT 
                address,
                project_name,
                label_subtype
            FROM near.core.dim_address_labels
            WHERE label_type = 'cex'
        ),

        transfers AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as day_,
                t.from_address,
                t.to_address,
                t.amount,
                t.amount_usd,
                COALESCE(from_cex.project_name, 'Unknown') as from_cex_name,
                COALESCE(to_cex.project_name, 'Unknown') as to_cex_name,
                CASE 
                    WHEN from_cex.address IS NOT NULL AND to_cex.address IS NULL THEN 'outbound'
                    WHEN from_cex.address IS NULL AND to_cex.address IS NOT NULL THEN 'inbound'
                    ELSE 'internal' 
                END as transfer_type
            FROM near.core.ez_token_transfers t
            LEFT JOIN cex_addresses from_cex ON t.from_address = from_cex.address
            LEFT JOIN cex_addresses to_cex ON t.to_address = to_cex.address
            WHERE day_ >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.central_exchange_flows)),
                '1970-01-01'
            )
            AND t.from_address != 'system'
            AND (from_cex.address IS NOT NULL OR to_cex.address IS NOT NULL)
        ),

        daily_volumes AS (
            SELECT 
                day_,
                CASE 
                    WHEN transfer_type = 'outbound' THEN from_cex_name
                    WHEN transfer_type = 'inbound' THEN to_cex_name
                    ELSE from_cex_name
                END as exchange_name,
                SUM(CASE WHEN transfer_type = 'inbound' THEN amount_usd ELSE 0 END) as inbound_volume_usd,
                SUM(CASE WHEN transfer_type = 'outbound' THEN amount_usd ELSE 0 END) as outbound_volume_usd,
                SUM(CASE 
                    WHEN transfer_type = 'inbound' THEN amount_usd 
                    WHEN transfer_type = 'outbound' THEN -amount_usd
                    ELSE 0 
                END) as net_volume_usd,
                SUM(CASE 
                    WHEN transfer_type IN ('inbound', 'outbound') THEN amount_usd
                    ELSE 0 
                END) as total_volume_usd
            FROM transfers
            WHERE transfer_type != 'internal'
            GROUP BY 1, 2
        )

        SELECT 
            day_,
            exchange_name,
            ROUND(inbound_volume_usd, 2) as inbound_volume_usd,
            ROUND(outbound_volume_usd, 2) as outbound_volume_usd,
            ROUND(net_volume_usd, 2) as net_volume_usd,
            ROUND(total_volume_usd, 2) as total_volume_usd
        FROM daily_volumes
    ) AS source
    ON target.day_ = source.day_ 
    AND target.exchange_name = source.exchange_name
    WHEN MATCHED THEN
        UPDATE SET 
            target.inbound_volume_usd = source.inbound_volume_usd,
            target.outbound_volume_usd = source.outbound_volume_usd,
            target.net_volume_usd = source.net_volume_usd,
            target.total_volume_usd = source.total_volume_usd
    WHEN NOT MATCHED THEN
        INSERT (day_, exchange_name, inbound_volume_usd, outbound_volume_usd, net_volume_usd, total_volume_usd)
        VALUES (source.day_, source.exchange_name, source.inbound_volume_usd, source.outbound_volume_usd, source.net_volume_usd, source.total_volume_usd);
    
    RETURN 'NEAR exchange flows updated successfully';
END;
$$;

-- Create task to update exchange flows every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_central_exchange_flows_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_central_exchange_flows();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_central_exchange_flows_task RESUME;

-- Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.central_exchange_flows TO ROLE INTERNAL_DEV;

-- Individual access for Velocity Ethereum
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.central_exchange_flows TO ROLE VELOCITY_ETHEREUM;