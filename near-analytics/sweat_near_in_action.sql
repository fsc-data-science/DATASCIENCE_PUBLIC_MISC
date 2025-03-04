select * from datascience_public_misc.near_analytics.sweat_near_in_action;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.near_analytics;

-- Step 2: Create table based on observation level (day-level)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.sweat_near_in_action (
    day_ DATE,
    near_bought FLOAT,
    near_deposited FLOAT,
    near_traded_for_staked_near FLOAT,
    near_in_action FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_sweat_near_in_action();

-- Step 4: Define the procedure with a 90-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_sweat_near_in_action()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    MERGE INTO datascience_public_misc.near_analytics.sweat_near_in_action AS target
    USING (
        WITH active_users AS (
            SELECT DISTINCT
                CASE WHEN tx_signer = 'sweat-relayer.near' THEN tx_receiver ELSE tx_signer END AS user_
            FROM near.core.fact_transactions 
            WHERE (
                tx_signer = 'sweat-relayer.near'
                OR tx_signer IN (SELECT sweat_relay_user FROM datascience_public_misc.near_analytics.sweat_relay_usage)
                OR tx_signer IN (SELECT sweat_receiver FROM datascience_public_misc.near_analytics.sweat_welcome_transfers)
            )
            HAVING user_ IN (
                SELECT sweat_relay_user FROM datascience_public_misc.near_analytics.sweat_relay_usage 
                UNION ALL 
                SELECT sweat_receiver FROM datascience_public_misc.near_analytics.sweat_welcome_transfers
            )
        ),

        near_dex_buys AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as select_day_,
                SUM(CASE WHEN token_out_contract = 'wrap.near' THEN amount_out ELSE 0 END) as near_bought
            FROM near.defi.ez_dex_swaps
            WHERE block_timestamp >= current_date - 90
            AND token_out_contract = 'wrap.near'
            AND (
                trader IN (SELECT user_ FROM active_users)
                OR trader = 'sweat-relayer.near'
            )
            GROUP BY 1
        ),

        near_lending_deposits AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as select_day_,
                SUM(CASE WHEN symbol = 'wNEAR' AND actions = 'deposit' THEN amount ELSE 0 END) as near_deposited
            FROM near.defi.ez_lending
            WHERE block_timestamp >= current_date - 90
            AND symbol = 'wNEAR'
            AND actions = 'deposit'
            AND (
                sender_id IN (SELECT user_ FROM active_users)
                OR sender_id = 'sweat-relayer.near'
            )
            GROUP BY 1
        ),

        near_to_staked AS (
            SELECT 
                DATE_TRUNC('day', block_timestamp) as select_day_,
                SUM(amount_in) as near_to_staked
            FROM near.defi.ez_dex_swaps
            WHERE block_timestamp >= current_date - 90
            AND token_in_contract = 'wrap.near'
            AND token_out_contract IN ('linear-protocol.near', 'meta-pool.near')
            AND (
                trader IN (SELECT user_ FROM active_users)
                OR trader = 'sweat-relayer.near'
            )
            GROUP BY select_day_
        )

        SELECT 
            COALESCE(b.select_day_, d.select_day_, s.select_day_) as day_,
            SUM(COALESCE(near_bought, 0)) as near_bought_,
            SUM(COALESCE(near_deposited, 0)) as near_deposited_,
            SUM(COALESCE(near_to_staked, 0)) as near_traded_for_staked_near_,
            near_bought_ + near_deposited_ + near_traded_for_staked_near_ as near_in_action
        FROM near_dex_buys b
        FULL JOIN near_lending_deposits d ON b.select_day_ = d.select_day_
        FULL JOIN near_to_staked s ON COALESCE(b.select_day_, d.select_day_) = s.select_day_ 
        GROUP BY day_

    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            near_bought = source.near_bought_,
            near_deposited = source.near_deposited_,
            near_traded_for_staked_near = source.near_traded_for_staked_near_,
            near_in_action = source.near_in_action

    WHEN NOT MATCHED THEN
        INSERT (day_, near_bought, near_deposited, near_traded_for_staked_near, near_in_action)
        VALUES (
            source.day_,
            source.near_bought_,
            source.near_deposited_,
            source.near_traded_for_staked_near_,
            source.near_in_action

        );

    RETURN 'NEAR usage metrics updated successfully';
END;
$$;

-- Add clustering to the table for better query performance
ALTER TABLE datascience_public_misc.near_analytics.sweat_near_in_action
CLUSTER BY (day_);

-- Grant necessary permissions for Studio access
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_near_in_action TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.sweat_near_in_action TO ROLE VELOCITY_ETHEREUM;

-- Create task to update metrics every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_sweat_near_in_action_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.near_analytics.update_sweat_near_in_action();

-- Resume the task
ALTER TASK datascience_public_misc.near_analytics.update_sweat_near_in_action_task RESUME;