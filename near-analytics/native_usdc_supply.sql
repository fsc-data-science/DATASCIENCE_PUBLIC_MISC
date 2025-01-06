select * from 
datascience_public_misc.near_analytics.near_daily_usdc_supply
order by day_ desc;


-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_daily_usdc_supply (
    day_ DATE,
    token_ VARCHAR,
    amount_mint FLOAT DEFAULT 0,
    amount_burn FLOAT DEFAULT 0,
    net_change FLOAT DEFAULT 0,
    usdc_in_circulation FLOAT,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_near_daily_usdc_supply();

-- Step 4: Define the procedure with lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_near_daily_usdc_supply()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- First merge: Update all metrics except usdc_in_circulation
    MERGE INTO datascience_public_misc.near_analytics.near_daily_usdc_supply AS target
    USING (
        WITH date_spine AS (
            SELECT DATEADD(day, seq4(), '2022-11-28'::DATE)::DATE as day_
            FROM TABLE(GENERATOR(ROWCOUNT => 5000))
            WHERE day_ <= CURRENT_DATE
        ),
        
        daily_changes AS (
            SELECT
                date_trunc('day', block_timestamp)::DATE as day_,
                'Native USDC' as token_,
                sum(CASE WHEN method_name = 'mint' THEN DIV0(args:amount::float, 1e6) ELSE 0 END) as amount_mint,
                sum(CASE WHEN method_name = 'burn' THEN -1*DIV0(args:amount::float, 1e6) ELSE 0 END) as amount_burn,
                amount_mint + amount_burn as net_change
            FROM near.core.fact_actions_events_function_call 
            WHERE receiver_id = '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1'
                AND receipt_succeeded = 'TRUE'
                AND method_name IN ('mint','burn')
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdc_supply)),
                    '2022-11-28'::DATE
                )
            GROUP BY day_, token_
        ),
        
        filled_changes AS (
            SELECT 
                d.day_,
                'Native USDC' as token_,
                COALESCE(dc.amount_mint, 0) as amount_mint,
                COALESCE(dc.amount_burn, 0) as amount_burn,
                COALESCE(dc.net_change, 0) as net_change
            FROM date_spine d
            LEFT JOIN daily_changes dc
                ON d.day_ = dc.day_
        )
        
        SELECT * FROM filled_changes
    ) AS source
    ON target.day_ = source.day_ AND target.token_ = source.token_
    WHEN MATCHED THEN
        UPDATE SET 
            target.amount_mint = source.amount_mint,
            target.amount_burn = source.amount_burn,
            target.net_change = source.net_change,
            target._updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (day_, token_, amount_mint, amount_burn, net_change)
        VALUES (
            source.day_, 
            source.token_, 
            source.amount_mint, 
            source.amount_burn, 
            source.net_change
        );

    -- Second update: Recalculate all usdc_in_circulation values
    UPDATE datascience_public_misc.near_analytics.near_daily_usdc_supply t
    SET usdc_in_circulation = s.running_total,
        _updated_timestamp = CURRENT_TIMESTAMP()
    FROM (
        SELECT 
            day_,
            token_,
            SUM(net_change) OVER (ORDER BY day_ ASC) as running_total
        FROM datascience_public_misc.near_analytics.near_daily_usdc_supply
    ) s
    WHERE t.day_ = s.day_ AND t.token_ = s.token_;

    RETURN 'NEAR daily USDC supply metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.near_daily_usdc_supply
CLUSTER BY (day_);

-- Create task to update data every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_usdc_supply_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS 
  CALL datascience_public_misc.near_analytics.update_near_daily_usdc_supply();
-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_near_daily_usdc_supply_task RESUME;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_usdc_supply TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_usdc_supply TO ROLE VELOCITY_ETHEREUM;