
select *
from datascience_public_misc.near_analytics.sweat_users_daily_token_net_change
where day_ >= current_date - 5
limit 100;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change (
    day_ DATE,
    user_ VARCHAR,
    contract_address VARCHAR,
    symbol VARCHAR,
    transfer_in FLOAT,
    transfer_out FLOAT,
    tx_fees_paid_in_token FLOAT,
    net_change FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_sweat_users_daily_token_net_change();

-- Step 4: Define the procedure with a 3-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_sweat_users_daily_token_net_change()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.sweat_users_daily_token_net_change AS target
    USING (
        WITH outbound_transfers AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                from_address as user_,
                'out' as direction_,
                contract_address,
                symbol,
                sum(amount) as amount
            FROM near.core.ez_token_transfers t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON t.from_address = s.sweat_receiver
            WHERE contract_address IN (
                'wrap.near',
                'token.sweat',
                '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
                'meta-pool.near',
                'usdt.tether-token.near',
                'linear-protocol.near',
                'aurora'
            )
            AND amount >= 0
            AND s.is_first_sweat_receive = 1
            AND t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, direction_, contract_address, symbol 
        ),
        
        inbound_transfers AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                to_address as user_,
                'in' as direction_,
                contract_address,
                symbol,
                sum(amount) as amount
            FROM near.core.ez_token_transfers t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON t.to_address = s.sweat_receiver
            WHERE contract_address IN (
                'wrap.near',
                'token.sweat',
                '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
                'meta-pool.near',
                'usdt.tether-token.near',
                'linear-protocol.near',
                'aurora'
            )
            AND amount >= 0
                        AND s.is_first_sweat_receive = 1
            AND t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, direction_, contract_address, symbol 
        ),
        
        all_transfers AS (
            SELECT * FROM outbound_transfers
            UNION ALL 
            SELECT * FROM inbound_transfers
        ),
        
        daily_transfers AS (
            SELECT 
                day_,
                user_,
                contract_address,
                symbol,
                SUM(CASE WHEN direction_ = 'in' THEN amount ELSE 0 END) as transfer_in,
                SUM(CASE WHEN direction_ = 'out' THEN amount ELSE 0 END) as transfer_out
            FROM all_transfers
            GROUP BY day_, user_, contract_address, symbol
        ),

        daily_direct_fees AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                tx_signer as user_,
                'wrap.near' as contract_address,
                'wNEAR' as symbol,
                SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
            FROM near.core.fact_transactions t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON tx_signer = s.sweat_receiver
            WHERE t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, contract_address, symbol
        ),
       
        daily_relay_fees AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                tx_receiver as user_,
                'wrap.near' as contract_address,
                'wNEAR' as symbol,
                SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
            FROM near.core.fact_transactions t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON tx_receiver = s.sweat_receiver
            WHERE tx_signer = 'sweat-relayer.near'
            AND t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, contract_address, symbol
        ),

        combined_fees AS (
            SELECT 
                day_, 
                user_, 
                contract_address, 
                symbol,
                SUM(tx_fees_paid_in_token) as tx_fees_paid_in_token
            FROM (
                SELECT * FROM daily_direct_fees
                UNION ALL
                SELECT * FROM daily_relay_fees
            )
            GROUP BY day_, user_, contract_address, symbol
        ),
        
        final_data AS (
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
            FULL JOIN combined_fees f 
                ON t.day_ = f.day_ 
                AND t.user_ = f.user_
                AND t.contract_address = f.contract_address
        )
        
        SELECT * FROM final_data
    ) AS source
    ON target.day_ = source.day_ 
        AND target.user_ = source.user_
        AND target.contract_address = source.contract_address
        AND target.symbol = source.symbol
    WHEN MATCHED THEN
        UPDATE SET 
            transfer_in = source.transfer_in,
            transfer_out = source.transfer_out,
            tx_fees_paid_in_token = source.tx_fees_paid_in_token,
            net_change = source.net_change
    WHEN NOT MATCHED THEN
        INSERT (day_, user_, contract_address, symbol, transfer_in, transfer_out, tx_fees_paid_in_token, net_change)
        VALUES (source.day_, source.user_, source.contract_address, source.symbol, 
                source.transfer_in, source.transfer_out, source.tx_fees_paid_in_token, source.net_change);

    RETURN 'SWEAT users daily token net change updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change
CLUSTER BY (day_, contract_address);

-- Create task to update token activity every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_sweat_users_daily_token_net_change_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_sweat_users_daily_token_net_change();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_sweat_users_daily_token_net_change_task RESUME;

-- Set appropriate permissions for Studio access
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change TO ROLE VELOCITY_ETHEREUM;

-- Grant permissions to INTERNAL_DEV role
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change TO ROLE INTERNAL_DEV;