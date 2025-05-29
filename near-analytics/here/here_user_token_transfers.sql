select 
user_,
contract_address,
transfer_type,
sum(transfer_in - transfer_out - tx_fees_paid_in_token) as estimated_balance
from 
datascience_public_misc.near_analytics.here_users_daily_token_net_change
group by 1, 2, 3
order by 4 asc
;

select 
user_,
contract_address,
symbol,
transfer_type,
sum(transfer_in - transfer_out - tx_fees_paid_in_token) as estimated_balance
 from datascience_public_misc.near_analytics.here_users_daily_token_net_change
group by 1, 2, 3, 4
order by 5 asc
;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on user-day-contract observation level
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.here_users_daily_token_net_change (
    day_ DATE,
    user_ STRING,
    contract_address STRING,
    symbol STRING,
    transfer_type STRING,
    transfer_in FLOAT DEFAULT 0,
    transfer_out FLOAT DEFAULT 0,
    tx_fees_paid_in_token FLOAT DEFAULT 0
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_here_users_daily_token_net_change();

-- Step 4: Define the procedure (incremental, 3-day lookback, transfer_type, wrap.near only, no net_change)
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_here_users_daily_token_net_change()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.here_users_daily_token_net_change AS target
    USING (
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
                date_trunc('day', t.block_timestamp) as day_,
                t.from_address as user_,
                t.contract_address,
                t.symbol,
               t.transfer_type,
                'out' as direction_,
                SUM(t.amount) as amount
            FROM near.core.ez_token_transfers t
            INNER JOIN here_users h 
                ON t.from_address = h.here_user
            WHERE t.contract_address = 'wrap.near'
            and t.from_address != 'system'
            and t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.here_users_daily_token_net_change)),
                '1970-01-01'
            )
            AND t.amount >= 0
            GROUP BY day_, user_, contract_address, symbol, direction_, transfer_type
        ),
        inbound_transfers AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                t.to_address as user_,
                t.contract_address,
                t.symbol,
                t.transfer_type,
                'in' as direction_,
                SUM(t.amount) as amount
            FROM near.core.ez_token_transfers t
            INNER JOIN here_users h 
                ON t.to_address = h.here_user
            WHERE t.contract_address = 'wrap.near'
            and t.from_address != 'system'
            and t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.here_users_daily_token_net_change)),
                '1970-01-01'
            )
            AND t.amount >= 0
            GROUP BY day_, user_, contract_address, symbol, direction_, transfer_type
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
        daily_fees AS (
            SELECT 
                date_trunc('day', t.block_timestamp) as day_,
                t.tx_signer as user_,
                'wrap.near' as contract_address,
                'wNEAR' as symbol,
                'native' as transfer_type,
                SUM(DIV0(t.transaction_fee, 1e24)) as tx_fees_paid_in_token
            FROM near.core.fact_transactions t
            INNER JOIN here_users h
                ON t.tx_signer = h.here_user
            WHERE t.block_timestamp >= COALESCE(
                DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.here_users_daily_token_net_change)),
                '1970-01-01'
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
             INNER JOIN here_users h
                ON s.trader = h.here_user
            WHERE s.token_in_contract = 'wrap.near'
              AND s.block_timestamp >= COALESCE(
                    DATEADD(day, -3, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.here_users_daily_token_net_change)),
                    '1970-01-01'
                )
            GROUP BY day_, user_, contract_address, symbol, transfer_type
        ),
        final_data AS (
            SELECT 
                COALESCE(t.day_, f.day_, s.day_) as day_,
                COALESCE(t.user_, f.user_, s.user_) as user_,
                COALESCE(t.contract_address, f.contract_address, s.contract_address) as contract_address,
                COALESCE(t.symbol, f.symbol, s.symbol) as symbol,
                COALESCE(t.transfer_type, f.transfer_type, s.transfer_type) as transfer_type,
                CASE 
                    WHEN COALESCE(t.contract_address, f.contract_address, s.contract_address) = 'wrap.near'
                     AND COALESCE(t.transfer_type, f.transfer_type, s.transfer_type) = 'nep141'
                    THEN COALESCE(t.transfer_in, 0) - COALESCE(s.amount_sold, 0)
                    ELSE COALESCE(t.transfer_in, 0)
                END as transfer_in,
                COALESCE(t.transfer_out, 0) as transfer_out,
                COALESCE(f.tx_fees_paid_in_token, 0) as tx_fees_paid_in_token
            FROM daily_transfers t
            FULL OUTER JOIN daily_fees f 
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
    ) AS src
    ON target.day_ = src.day_
    AND target.user_ = src.user_
    AND target.contract_address = src.contract_address
    AND target.symbol = src.symbol
    AND target.transfer_type = src.transfer_type
    WHEN MATCHED THEN UPDATE SET
        transfer_in = src.transfer_in,
        transfer_out = src.transfer_out,
        tx_fees_paid_in_token = src.tx_fees_paid_in_token
    WHEN NOT MATCHED THEN INSERT (
        day_, user_, contract_address, symbol, transfer_type, transfer_in, transfer_out, tx_fees_paid_in_token
    ) VALUES (
        src.day_, src.user_, src.contract_address, src.symbol, src.transfer_type, src.transfer_in, src.transfer_out, src.tx_fees_paid_in_token
    );

    RETURN 'HERE users daily transfers incrementally updated (3-day lookback)';
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