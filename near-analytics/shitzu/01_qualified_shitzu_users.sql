select max(first_activity) from datascience_public_misc.near_analytics.shitzu_users;


-- Step 2: Create table based on observation level (user-level)
CREATE OR REPLACE TABLE datascience_public_misc.near_analytics.shitzu_users (
    user_ VARCHAR,
    first_activity TIMESTAMP
);

-- Step 3: Call the procedure
CALL datascience_public_misc.near_analytics.update_shitzu_users();

-- Step 4: Define the procedure with an intelligent lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.near_analytics.update_shitzu_users()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
MERGE INTO datascience_public_misc.near_analytics.shitzu_users AS target
USING (
    WITH traders AS (
        SELECT 
            trader AS user_,
            MIN(block_timestamp) AS first_trade
        FROM near.defi.ez_dex_swaps
        WHERE (token_in_contract = 'token.0xshitzu.near' OR token_out_contract = 'token.0xshitzu.near')
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY trader
    ),
    stakers AS (
        SELECT 
            signer_id AS user_,
            MIN(block_timestamp) AS first_stake
        FROM near.gov.fact_staking_actions 
        WHERE address IN ('shitzu.pool.near','shitzuapes.pool.near')
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY signer_id
    ),
    shitzu_meme_season_stakers AS (
        SELECT 
            from_address AS user_,
            MIN(block_timestamp) AS first_meme_season_stake
        FROM near.core.ez_token_transfers
        WHERE to_address = 'meme-farming_011.ref-labs.near'
        AND contract_address = 'token.0xshitzu.near'
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY from_address
    ),
    meme_season_claimers AS (
        SELECT 
            tx_signer AS user_,
            MIN(block_timestamp) AS first_claim
        FROM near.core.ez_actions
        WHERE tx_receiver = 'memeseason.0xshitzu.near'
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY tx_signer
    ),
    nft_collection_buyers AS (
        SELECT 
            buyer_address AS user_,
            MIN(block_timestamp) AS first_buy
        FROM near.nft.ez_nft_sales
        WHERE nft_address = 'shitzu.bodega-lab.near'
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY buyer_address
    ),
    nft_collection_sellers AS (
        SELECT 
            seller_address AS user_,
            MIN(block_timestamp) AS first_sell
        FROM near.nft.ez_nft_sales
        WHERE nft_address = 'shitzu.bodega-lab.near'
        AND block_timestamp >= COALESCE(
            DATEADD(day, -1, (SELECT MAX(first_activity) FROM datascience_public_misc.near_analytics.shitzu_users)),
            '1970-01-01'
        )
        GROUP BY seller_address
    )
    SELECT 
        DISTINCT user_,
        LEAST(
            COALESCE(first_trade, '9999-12-31'::TIMESTAMP),
            COALESCE(first_stake, '9999-12-31'::TIMESTAMP),
            COALESCE(first_meme_season_stake, '9999-12-31'::TIMESTAMP),
            COALESCE(first_claim, '9999-12-31'::TIMESTAMP),
            COALESCE(first_buy, '9999-12-31'::TIMESTAMP),
            COALESCE(first_sell, '9999-12-31'::TIMESTAMP)
        ) AS first_activity
    FROM traders 
    FULL JOIN stakers USING (user_)
    FULL JOIN shitzu_meme_season_stakers USING (user_)
    FULL JOIN meme_season_claimers USING (user_)
    FULL JOIN nft_collection_buyers USING (user_)
    FULL JOIN nft_collection_sellers USING (user_)
    WHERE user_ IS NOT NULL
    AND user_ not in (
        '0-relay.hot.tg',
        'relay.tg',
        'sweat-relayer.near',
        'oktest.near',
        'oct-stake-1.near',
        'oct-stake.near',
        'operator.meta-pool.near',
        'solver-ref.near',
        'operator-linear-staging.near',
        'operator.linear-protocol.near',
        'operator.meta-pool.near'
    )
) AS source
ON target.user_ = source.user_
WHEN MATCHED THEN
    UPDATE SET target.first_activity = LEAST(target.first_activity, source.first_activity)
WHEN NOT MATCHED THEN
    INSERT (user_, first_activity)
    VALUES (source.user_, source.first_activity);

RETURN 'Shitzu users updated successfully';
END;
$$;


-- Step 5: Create task to update Shitzu users every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.near_analytics.update_shitzu_users_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
  CALL datascience_public_misc.near_analytics.update_shitzu_users();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.near_analytics.update_shitzu_users_task RESUME;

-- Step 6: Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.near_analytics.shitzu_users TO ROLE INTERNAL_DEV;

-- Individual access for VELOCITY_ETHEREUM role
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.near_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.near_analytics.shitzu_users TO ROLE VELOCITY_ETHEREUM;