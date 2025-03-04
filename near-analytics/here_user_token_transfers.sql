select * from 
datascience_public_misc.near_analytics.here_users_daily_token_net_change
limit 10;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on user-day-contract observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change (
    day_ DATE,
    user_ STRING,
    contract_address STRING,
    symbol STRING,
    transfer_in FLOAT DEFAULT 0,
    transfer_out FLOAT DEFAULT 0,
    tx_fees_paid_in_token FLOAT DEFAULT 0,
    net_change FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_here_users_daily_token_net_change();

-- Step 4: Define the procedure
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_here_users_daily_token_net_change()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate existing data
    TRUNCATE TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change;
    
    -- Insert fresh data
    INSERT INTO datascience_public_misc.near_analytics.here_users_daily_token_net_change (
        day_, 
        user_, 
        contract_address, 
        symbol, 
        transfer_in, 
        transfer_out, 
        tx_fees_paid_in_token, 
        net_change
    )
    WITH here_users AS (
        SELECT DISTINCT here_user 
        FROM (
            SELECT here_relay_user as here_user
            FROM datascience_public_misc.near_analytics.here_relay_usage
            GROUP BY here_user
            UNION ALL
            SELECT direct_tg_signer as here_user
            FROM datascience_public_misc.near_analytics.heretg_first_tx
        )
    ),
    
    outbound_transfers AS (
        SELECT 
            t.block_timestamp,
            t.from_address as user_,
            t.contract_address,
            t.symbol,
            t.amount,
            'out' as direction_
        FROM near.core.ez_token_transfers t
        INNER JOIN here_users h 
            ON t.from_address = h.here_user
        WHERE contract_address IN (
            'wrap.near',
            '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
            'token.burrow.near',
            'meta-pool.near',
            'usdt.tether-token.near',
            'linear-protocol.near',
            'aurora'
        )
        AND amount >= 0
    ),
    
    inbound_transfers AS (
        SELECT 
            t.block_timestamp,
            t.to_address as user_,
            t.contract_address,
            t.symbol,
            t.amount,
            'in' as direction_
        FROM near.core.ez_token_transfers t
        INNER JOIN here_users h 
            ON t.to_address = h.here_user
        WHERE contract_address IN (
            'wrap.near',
            '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
            'token.burrow.near',
            'meta-pool.near',
            'usdt.tether-token.near',
            'linear-protocol.near',
            'aurora'
        )
        AND amount >= 0
    ),
    
    all_transfers AS (
        SELECT * FROM outbound_transfers
        UNION ALL 
        SELECT * FROM inbound_transfers
    ),
    
    daily_transfers AS (
        SELECT 
            date_trunc('day', block_timestamp)::DATE as day_,
            user_,
            contract_address,
            symbol,
            SUM(CASE WHEN direction_ = 'in' THEN amount ELSE 0 END) as transfer_in,
            SUM(CASE WHEN direction_ = 'out' THEN amount ELSE 0 END) as transfer_out
        FROM all_transfers
        GROUP BY 1,2,3,4
    ),
    
    daily_fees AS (
        SELECT 
            date_trunc('day', t.block_timestamp)::DATE as day_,
            t.tx_signer as user_,
            'wrap.near' as contract_address,
            'wNEAR' as symbol,
            SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
        FROM near.core.fact_transactions t
        INNER JOIN here_users h
            ON t.tx_signer = h.here_user
        GROUP BY 1,2,3,4
    )
    
    SELECT 
        COALESCE(t.day_, f.day_) as day_,
        COALESCE(t.user_, f.user_) as user_,
        COALESCE(t.contract_address, f.contract_address) as contract_address,
        COALESCE(t.symbol, f.symbol) as symbol,
        COALESCE(t.transfer_in, 0) as transfer_in,
        COALESCE(t.transfer_out, 0) as transfer_out,
        COALESCE(f.tx_fees_paid_in_token, 0) as tx_fees_paid_in_token,
        transfer_in - transfer_out - 
            CASE WHEN COALESCE(t.contract_address, f.contract_address) = 'wrap.near' 
                 THEN COALESCE(f.tx_fees_paid_in_token, 0) 
                 ELSE 0 
            END as net_change
    FROM daily_transfers t
    FULL OUTER JOIN daily_fees f 
        ON t.day_ = f.day_ 
        AND t.user_ = f.user_
        AND t.contract_address = f.contract_address
    WHERE COALESCE(t.transfer_in, 0) != 0 
       OR COALESCE(t.transfer_out, 0) != 0
       OR COALESCE(f.tx_fees_paid_in_token, 0) != 0;

    RETURN 'HERE users daily transfers updated successfully';
END;
$$;

-- Add clustering to improve query performance
ALTER TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change
CLUSTER BY (day_, contract_address);

-- Create task to update every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_here_users_daily_token_net_change_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_here_users_daily_token_net_change();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_here_users_daily_token_net_change_task RESUME;

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change TO ROLE INTERNAL_DEV;

GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change TO ROLE VELOCITY_ETHEREUM;