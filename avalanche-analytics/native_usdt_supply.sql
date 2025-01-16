select * ,
sum(net_change) over (order by day_ asc) as cumulative_net_change,
sum(treasury_inflow) over (order by day_ asc) as cumulative_treasury_inflow,
sum(treasury_outflow) over (order by day_ asc) as cumulative_treasury_outflow,
cumulative_net_change - cumulative_treasury_inflow + cumulative_treasury_outflow as circulating_supply
from datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply
order by day_ desc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.avalanche_analytics;

-- Step 2: Create table based on observation level (day-level)
CREATE OR REPLACE TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply (
    day_ TIMESTAMP,
    amount_mint FLOAT,
    amount_burn FLOAT,
    net_change FLOAT,
    treasury_inflow FLOAT,
    treasury_outflow FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply();

-- Step 4: Define the procedure with a 2-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply AS target
    USING (
        WITH mint_burn_events AS (
            SELECT 
                block_timestamp,
                tx_hash,
                CASE 
                    WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint' 
                    ELSE 'burn' 
                END as event_,
                CASE 
                    WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount 
                    ELSE -amount 
                END as amount_signed
            FROM avalanche.core.ez_token_transfers
            WHERE contract_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7'
                AND (from_address = '0x0000000000000000000000000000000000000000'
                     OR to_address = '0x0000000000000000000000000000000000000000')
                AND DATE_TRUNC('day', block_timestamp) >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply)),
                    '1970-01-01'
                )
        ),
        daily_mint_burn AS (
            SELECT 
                date_trunc('day', block_timestamp) as day_,
                SUM(CASE WHEN event_ = 'mint' THEN amount_signed ELSE 0 END) as amount_mint,
                SUM(CASE WHEN event_ = 'burn' THEN amount_signed ELSE 0 END) as amount_burn,
                SUM(amount_signed) as net_change
            FROM mint_burn_events
            GROUP BY 1
        ),
        treasury_flows AS (
            SELECT 
                date_trunc('day', block_timestamp) as day_,
                SUM(CASE 
                    WHEN to_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
                    THEN amount ELSE 0 END) as treasury_inflow,
                SUM(CASE 
                    WHEN from_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
                    THEN amount ELSE 0 END) as treasury_outflow
            FROM avalanche.core.ez_token_transfers
            WHERE (to_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
                   OR from_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949')
                AND contract_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7'
                AND DATE_TRUNC('day', block_timestamp) >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply)),
                    '1970-01-01'
                )
            GROUP BY 1
        )
        SELECT 
            COALESCE(m.day_, t.day_) as day_,
            COALESCE(m.amount_mint, 0) as amount_mint,
            COALESCE(m.amount_burn, 0) as amount_burn,
            COALESCE(m.net_change, 0) as net_change,
            COALESCE(t.treasury_inflow, 0) as treasury_inflow,
            COALESCE(t.treasury_outflow, 0) as treasury_outflow
        FROM daily_mint_burn m
        FULL JOIN treasury_flows t 
            ON m.day_ = t.day_
        ORDER BY day_ DESC
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET
            amount_mint = source.amount_mint,
            amount_burn = source.amount_burn,
            net_change = source.net_change,
            treasury_inflow = source.treasury_inflow,
            treasury_outflow = source.treasury_outflow
    WHEN NOT MATCHED THEN
        INSERT (
            day_,
            amount_mint,
            amount_burn,
            net_change,
            treasury_inflow,
            treasury_outflow
        )
        VALUES (
            source.day_,
            source.amount_mint,
            source.amount_burn,
            source.net_change,
            source.treasury_inflow,
            source.treasury_outflow
        );

    RETURN 'AVAX daily USDT supply metrics updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply
CLUSTER BY (day_);

-- Create task to update AVAX USDT supply metrics every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
  CALL datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply_task RESUME;

-- Grant permissions
GRANT USAGE ON SCHEMA datascience_public_misc.avalanche_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON PROCEDURE datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply() TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TASK datascience_public_misc.avalanche_analytics.update_avax_daily_usdt_supply_task TO ROLE INTERNAL_DEV;

-- Individual access for VELOCITY_ETHEREUM
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.avalanche_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdt_supply TO ROLE VELOCITY_ETHEREUM;