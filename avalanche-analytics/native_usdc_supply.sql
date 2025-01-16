select *, 
sum(net_change) over (order by day_ asc) as usdc_in_circulation
from datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply
order by day_ desc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.avalanche_analytics;

-- Step 2: Create table based on observation level (day)
CREATE OR REPLACE TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply (
    day_ DATE,
    token_ VARCHAR,
    amount_mint FLOAT DEFAULT 0,
    amount_burn FLOAT DEFAULT 0,
    net_change FLOAT DEFAULT 0
);

call datascience_public_misc.avalanche_analytics.update_avax_daily_usdc_supply();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.avalanche_analytics.update_avax_daily_usdc_supply()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply AS target
    USING (
        WITH daily_mints_burns AS (
            SELECT
                date_trunc('day', block_timestamp) as day_,
                'Native USDC' as token_,
                SUM(CASE 
                    WHEN event_name = 'Mint' 
                    THEN DIV0(decoded_log:amount::float, 1e6) 
                    ELSE 0 
                END) as amount_mint,
                SUM(CASE 
                    WHEN event_name = 'Burn' 
                    THEN -1 * DIV0(decoded_log:amount::float, 1e6) 
                    ELSE 0 
                END) as amount_burn
            FROM avalanche.core.ez_decoded_event_logs
            WHERE contract_address = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e' -- Avalanche USDC
                AND event_name IN ('Mint', 'Burn')
                AND block_timestamp >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply)),
                    '2021-01-01'::DATE -- Set appropriate start date for Avalanche USDC
                )
            GROUP BY 1, 2
        )
        SELECT 
            day_,
            token_,
            amount_mint,
            amount_burn,
            amount_mint + amount_burn as net_change
        FROM daily_mints_burns
    ) AS source
    ON target.day_ = source.day_ AND target.token_ = source.token_
    WHEN MATCHED THEN
        UPDATE SET 
            target.amount_mint = source.amount_mint,
            target.amount_burn = source.amount_burn,
            target.net_change = source.net_change
    WHEN NOT MATCHED THEN
        INSERT (day_, token_, amount_mint, amount_burn, net_change)
        VALUES (
            source.day_, 
            source.token_, 
            source.amount_mint, 
            source.amount_burn, 
            source.net_change
        );

    RETURN 'Avalanche daily USDC supply metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply
CLUSTER BY (day_);

-- Create task to update data every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.avalanche_analytics.update_avax_daily_usdc_supply_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 45 3 * * * America/Los_Angeles'
AS 
  CALL datascience_public_misc.avalanche_analytics.update_avax_daily_usdc_supply();

-- Resume the task
ALTER TASK datascience_public_misc.avalanche_analytics.update_avax_daily_usdc_supply_task RESUME;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA datascience_public_misc.avalanche_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.avalanche_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.avalanche_analytics.avax_daily_usdc_supply TO ROLE VELOCITY_ETHEREUM;