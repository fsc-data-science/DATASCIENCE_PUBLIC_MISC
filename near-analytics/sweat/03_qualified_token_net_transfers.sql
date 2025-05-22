
select 
symbol, transfer_type,
sum(transfer_in - transfer_out - tx_fees_paid_in_token) as net_change
from datascience_public_misc.near_analytics.sweat_users_daily_token_net_change
group by 1, 2
;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change (
    day_ DATE,
    user_ VARCHAR,
    contract_address VARCHAR,
    symbol VARCHAR,
    transfer_type VARCHAR,
    transfer_in FLOAT,
    transfer_out FLOAT,
    tx_fees_paid_in_token FLOAT
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
                transfer_type,
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
            GROUP BY day_, user_, direction_, transfer_type, contract_address, symbol 
        ),
        
        inbound_transfers AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                to_address as user_,
                'in' as direction_,
                transfer_type,
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
            -- overpayments of gas not debited so this system credit causes overcounting 
            -- unfortunately overpayment refund NOT proportional to gas used vs gas attached 
            -- cannot reproduce system refund calculations at the tx/receipt level 
            and from_address != 'system' 
            AND s.is_first_sweat_receive = 1
            AND t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, direction_, transfer_type, contract_address, symbol 
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
                transfer_type,
                SUM(CASE WHEN direction_ = 'in' THEN amount ELSE 0 END) as transfer_in,
                SUM(CASE WHEN direction_ = 'out' THEN amount ELSE 0 END) as transfer_out
            FROM all_transfers
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),

        daily_direct_fees AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                tx_signer as user_,
                'wrap.near' as contract_address,
                'wNEAR' as symbol,
                'native' as transfer_type,
                SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
            FROM near.core.fact_transactions t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON tx_signer = s.sweat_receiver
            WHERE t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),
       
        daily_relay_fees AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                tx_receiver as user_,
                'wrap.near' as contract_address,
                'wNEAR' as symbol,
                'native' as transfer_type,
                SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
            FROM near.core.fact_transactions t
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users s 
                ON tx_receiver = s.sweat_receiver
            WHERE tx_signer = 'sweat-relayer.near'
            AND t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                '1970-01-01'
            )
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),

        combined_fees AS (
            SELECT 
                day_, 
                user_, 
                contract_address, 
                symbol,
                transfer_type,
                SUM(tx_fees_paid_in_token) as tx_fees_paid_in_token
            FROM (
                SELECT * FROM daily_direct_fees
                UNION ALL
                SELECT * FROM daily_relay_fees
            )
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),
        
        daily_wnear_sales AS (
            SELECT
                date_trunc('day', s.block_timestamp) AS day_,
                s.trader AS user_,
                'wrap.near' AS contract_address,
                'wNEAR' AS symbol,
                'nep141' AS transfer_type,
                SUM(s.amount_in) AS amount_sold
            FROM near.defi.ez_dex_swaps s
            INNER JOIN datascience_public_misc.near_analytics.qualified_sweat_users q
                ON s.trader = q.sweat_receiver
            WHERE s.token_in_contract = 'wrap.near'
              AND q.is_first_sweat_receive = 1
              AND s.block_timestamp >= COALESCE(
                    DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.sweat_users_daily_token_net_change)),
                    '1970-01-01'
                )
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),
        
        final_data AS (
            SELECT 
                COALESCE(t.day_, f.day_, s.day_) as day_,
                COALESCE(t.user_, f.user_, s.user_) as user_,
                COALESCE(t.contract_address, f.contract_address, s.contract_address) as contract_address,
                COALESCE(t.transfer_type, f.transfer_type, s.transfer_type) as transfer_type,
                COALESCE(t.symbol, f.symbol, s.symbol) as symbol,
                CASE 
                    WHEN COALESCE(t.contract_address, f.contract_address, s.contract_address) = 'wrap.near'
                     AND COALESCE(t.transfer_type, f.transfer_type, s.transfer_type) = 'nep141'
                    THEN COALESCE(t.transfer_in, 0) - COALESCE(s.amount_sold, 0)
                    ELSE COALESCE(t.transfer_in, 0)
                END as transfer_in,
                COALESCE(t.transfer_out, 0) as transfer_out,
                COALESCE(f.tx_fees_paid_in_token, 0) as tx_fees_paid_in_token
            FROM daily_transfers t
            FULL JOIN combined_fees f 
                ON t.day_ = f.day_ 
                AND t.user_ = f.user_
                AND t.contract_address = f.contract_address
                AND t.transfer_type = f.transfer_type
            LEFT JOIN daily_wnear_sales s
                ON COALESCE(t.day_, f.day_) = s.day_
                AND COALESCE(t.user_, f.user_) = s.user_
                AND COALESCE(t.contract_address, f.contract_address) = s.contract_address
                AND COALESCE(t.transfer_type, f.transfer_type) = s.transfer_type
                AND COALESCE(t.symbol, f.symbol) = s.symbol
        )
        
        SELECT * FROM final_data
    ) AS source
    ON target.day_ = source.day_ 
        AND target.user_ = source.user_
        AND target.contract_address = source.contract_address
        AND target.symbol = source.symbol
        AND target.transfer_type = source.transfer_type
    WHEN MATCHED THEN
        UPDATE SET 
            transfer_in = source.transfer_in,
            transfer_out = source.transfer_out,
            tx_fees_paid_in_token = source.tx_fees_paid_in_token
    WHEN NOT MATCHED THEN
        INSERT (day_, user_, contract_address, symbol, transfer_type, transfer_in, transfer_out, tx_fees_paid_in_token)
        VALUES (source.day_, source.user_, source.contract_address, source.symbol, source.transfer_type, 
                source.transfer_in, source.transfer_out, source.tx_fees_paid_in_token);

    RETURN 'SWEAT users daily token net change updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.sweat_users_daily_token_net_change
CLUSTER BY (day_, contract_address, transfer_type);

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