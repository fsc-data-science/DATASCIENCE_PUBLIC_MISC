select * from datascience_public_misc.near_analytics.near_daily_usdt_supply
order by day_ desc;

-- Step 1: Create schema if it doesn't exist

CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (day)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply (
    day_ DATE,
    total_authorized FLOAT DEFAULT 0,
    treasury_balance FLOAT DEFAULT 0,
    usdt_in_circulation FLOAT,
    _inserted_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
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
    -- First merge: Update base metrics
    MERGE INTO datascience_public_misc.near_analytics.near_daily_usdt_supply AS target
    USING (
        WITH date_spine AS (
            SELECT DATEADD(day, seq4(), '2023-06-30'::DATE)::DATE as day_
            FROM TABLE(GENERATOR(ROWCOUNT => 5000))
            WHERE day_ <= CURRENT_DATE
        ),
        
        authorized_supply AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(DIV0(args:amount::float, 1e6)) as total_authorized
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
        
        treasury_balance AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(CASE 
                    WHEN to_address = 'tether-treasury.near' THEN amount
                    WHEN from_address = 'tether-treasury.near' THEN -amount
                    ELSE 0 
                END) as daily_treasury_flow
            FROM near.core.ez_token_transfers
            WHERE (to_address = 'tether-treasury.near' OR from_address = 'tether-treasury.near')
                AND contract_address = 'usdt.tether-token.near'
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdt_supply)),
                    '2023-06-30'::DATE
                )
            GROUP BY 1
        ),
        
        filled_metrics AS (
            SELECT 
                d.day_,
                COALESCE(a.total_authorized, 0) as authorized_amount,
                COALESCE(t.daily_treasury_flow, 0) as treasury_flow
            FROM date_spine d
            LEFT JOIN authorized_supply a ON d.day_ = a.day_
            LEFT JOIN treasury_balance t ON d.day_ = t.day_
        )
        
        SELECT 
            day_,
            authorized_amount as total_authorized,
            treasury_flow as treasury_balance
        FROM filled_metrics
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            target.total_authorized = source.total_authorized,
            target.treasury_balance = source.treasury_balance,
            target._updated_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (day_, total_authorized, treasury_balance)
        VALUES (
            source.day_,
            source.total_authorized, 
            source.treasury_balance
        );

    -- Second update: Calculate running totals and circulation
    UPDATE datascience_public_misc.near_analytics.near_daily_usdt_supply t
    SET total_authorized = s.running_authorized,
        treasury_balance = s.running_treasury,
        usdt_in_circulation = s.running_authorized - s.running_treasury,
        _updated_timestamp = CURRENT_TIMESTAMP()
    FROM (
        SELECT 
            day_,
            SUM(total_authorized) OVER (ORDER BY day_ ASC) as running_authorized,
            SUM(treasury_balance) OVER (ORDER BY day_ ASC) as running_treasury
        FROM datascience_public_misc.near_analytics.near_daily_usdt_supply
    ) s
    WHERE t.day_ = s.day_;

    RETURN 'NEAR daily USDT supply metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply
CLUSTER BY (day_);

CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_near_daily_usdt_supply_task
 WAREHOUSE = 'DATA_SCIENCE'
 SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS 
 CALL datascience_public_misc.near_analytics.update_near_daily_usdt_supply();

ALTER TASK datascience_public_misc.near_analytics.update_near_daily_usdt_supply_task RESUME;

-- Grant necessary permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply TO ROLE INTERNAL_DEV;

-- Grant Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.near_daily_usdt_supply TO ROLE VELOCITY_ETHEREUM;