select *,
sum(authorized_amount) over (order by day_ asc) as usdt_authorized,
sum(treasury_inflow) over (order by day_ asc) as usdt_treasury_inflow,
sum(treasury_outflow) over (order by day_ asc) as usdt_treasury_outflow,
usdt_authorized - (usdt_treasury_inflow - usdt_treasury_outflow) as usdt_in_circulation
 from datascience_public_misc.near_analytics.near_daily_usdt_supply
order by day_ desc;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply (
    day_ DATE,
    authorized_amount FLOAT DEFAULT 0,
    treasury_inflow FLOAT DEFAULT 0,
    treasury_outflow FLOAT DEFAULT 0
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_near_daily_usdt_supply();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_near_daily_usdt_supply()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.near_daily_usdt_supply AS target
    USING (
        WITH authorized_supply AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(DIV0(args:amount::float, 1e6)) as authorized_amount
            FROM near.core.fact_actions_events_function_call
            WHERE method_name = 'mint'
                AND receipt_succeeded = TRUE
                AND signer_id = '867487d0e6545dc3e34824bbb8213a8b82b8b6248322f509628f68b73ace74e2'
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdt_supply)),
                    '2023-06-30'::DATE
                )
            GROUP BY 1
        ),
        
        treasury_flows AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(CASE WHEN to_address = 'tether-treasury.near' THEN amount ELSE 0 END) as treasury_inflow,
                SUM(CASE WHEN from_address = 'tether-treasury.near' THEN amount ELSE 0 END) as treasury_outflow
            FROM near.core.ez_token_transfers
            WHERE (to_address = 'tether-treasury.near' OR from_address = 'tether-treasury.near')
                AND contract_address = 'usdt.tether-token.near'
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdt_supply)),
                    '2023-06-30'::DATE
                )
            GROUP BY 1
        ),
        
        combined_metrics AS (
            SELECT 
                COALESCE(a.day_, t.day_) as day_,
                COALESCE(a.authorized_amount, 0) as authorized_amount,
                COALESCE(t.treasury_inflow, 0) as treasury_inflow,
                COALESCE(t.treasury_outflow, 0) as treasury_outflow
            FROM authorized_supply a
            FULL OUTER JOIN treasury_flows t 
                ON a.day_ = t.day_
            WHERE COALESCE(a.authorized_amount, 0) != 0 
               OR COALESCE(t.treasury_inflow, 0) != 0
               OR COALESCE(t.treasury_outflow, 0) != 0
        )
        
        SELECT * FROM combined_metrics
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            target.authorized_amount = source.authorized_amount,
            target.treasury_inflow = source.treasury_inflow,
            target.treasury_outflow = source.treasury_outflow
    WHEN NOT MATCHED THEN
        INSERT (day_, authorized_amount, treasury_inflow, treasury_outflow)
        VALUES (
            source.day_,
            source.authorized_amount,
            source.treasury_inflow,
            source.treasury_outflow
        );

    RETURN 'NEAR daily USDT supply metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply
CLUSTER BY (day_);

-- Create task to update data every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_usdt_supply_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS 
  CALL datascience_public_misc.near_analytics.update_near_daily_usdt_supply();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_near_daily_usdt_supply_task RESUME;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply TO ROLE VELOCITY_ETHEREUM;