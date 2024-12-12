-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (platform-day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.platform_bridge_flows_daily (
    day_ TIMESTAMP,
    platform VARCHAR,
    inbound_volume_usd FLOAT,
    outbound_volume_usd FLOAT,
    total_volume_usd FLOAT,
    net_volume_usd FLOAT,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering to optimize query performance
ALTER TABLE datascience_public_misc.near_analytics.platform_bridge_flows_daily
CLUSTER BY (day_);

-- Step 3: Call procedure
CALL datascience_public_misc.near_analytics.update_platform_bridge_flows_daily();

-- Step 4: Define procedure with 2-day lookback
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_platform_bridge_flows_daily()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.platform_bridge_flows_daily AS target
    USING (
        WITH wormhole_ AS (
            SELECT 
                block_timestamp,
                date_trunc('hour', block_timestamp) as hour_,
                CASE 
                    WHEN b.symbol = 'WETH' THEN 'ETH'
                    WHEN b.symbol = 'WBNB' THEN 'BNB'
                    ELSE b.symbol 
                END as adjusted_symbol,
                amount,
                direction,
                p.price,
                amount*p.price as amount_usd
            FROM near.defi.ez_bridge_activity b 
            LEFT JOIN crosschain.price.ez_prices_hourly p 
                ON date_trunc('hour', b.block_timestamp) = p.hour 
                AND CASE 
                    WHEN b.symbol = 'WETH' THEN 'ETH'
                    WHEN b.symbol = 'WBNB' THEN 'BNB'
                    ELSE b.symbol 
                END = p.symbol
            WHERE p.blockchain IN ('near','ethereum','bsc','solana')
                AND (p.IS_NATIVE = TRUE OR 
                    p.token_address = '0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4'
                    OR p.token_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')
                AND b.block_timestamp >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.platform_bridge_flows_daily)),
                    '2020-01-01'
                )
                AND b.platform = 'wormhole' 
                AND b.receipt_succeeded = TRUE 
                AND b.SYMBOL IN ('NEAR','WETH','WBNB','SOL','USDC')
        ),

        wormhole_daily AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as day_,
                'wormhole' as platform,
                SUM(CASE WHEN direction = 'inbound' THEN amount_usd ELSE 0 END) as inbound_volume_usd,
                SUM(CASE WHEN direction = 'outbound' THEN amount_usd ELSE 0 END) as outbound_volume_usd
            FROM wormhole_
            GROUP BY 1
        ),

        other_bridges_daily AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as day_,
                platform,
                SUM(CASE WHEN direction = 'inbound' AND amount_usd IS NOT NULL THEN amount_usd ELSE 0 END) as inbound_volume_usd,
                SUM(CASE WHEN direction = 'outbound' AND amount_usd IS NOT NULL THEN amount_usd ELSE 0 END) as outbound_volume_usd
            FROM near.defi.ez_bridge_activity
            WHERE block_timestamp >= COALESCE(
                DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.platform_bridge_flows_daily)),
                '2020-01-01'
                )
                AND platform != 'wormhole'
                AND platform != 'multichain'
                AND receipt_succeeded = TRUE
            GROUP BY 1, 2
        )

        SELECT 
            day_,
            platform,
            inbound_volume_usd,
            outbound_volume_usd,
            inbound_volume_usd + outbound_volume_usd as total_volume_usd,
            inbound_volume_usd - outbound_volume_usd as net_volume_usd,
            CURRENT_TIMESTAMP() as _inserted_timestamp,
            CURRENT_TIMESTAMP() as _updated_timestamp
        FROM (
            SELECT * FROM wormhole_daily
            UNION ALL
            SELECT * FROM other_bridges_daily
        )
    ) AS source
    ON target.day_ = source.day_ 
    AND target.platform = source.platform
    WHEN MATCHED THEN
        UPDATE SET 
            target.inbound_volume_usd = source.inbound_volume_usd,
            target.outbound_volume_usd = source.outbound_volume_usd,
            target.total_volume_usd = source.total_volume_usd,
            target.net_volume_usd = source.net_volume_usd,
            target._updated_timestamp = source._updated_timestamp
    WHEN NOT MATCHED THEN
        INSERT (
            day_, 
            platform, 
            inbound_volume_usd, 
            outbound_volume_usd, 
            total_volume_usd, 
            net_volume_usd,
            _inserted_timestamp,
            _updated_timestamp
        )
        VALUES (
            source.day_,
            source.platform,
            source.inbound_volume_usd,
            source.outbound_volume_usd,
            source.total_volume_usd,
            source.net_volume_usd,
            source._inserted_timestamp,
            source._updated_timestamp
        );

    RETURN 'Platform bridge flows updated successfully';
END;
$$;

-- Create task to update every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_platform_bridge_flows_daily_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_platform_bridge_flows_daily();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_platform_bridge_flows_daily_task RESUME;

-- Set appropriate permissions for Studio
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.platform_bridge_flows_daily TO ROLE VELOCITY_ETHEREUM;