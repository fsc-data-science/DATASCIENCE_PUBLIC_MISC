-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (dex-lifetime stats)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.dex_stats (
    dex_name VARCHAR,
    swappers INTEGER,
    swap_txs INTEGER,
    amount_usd FLOAT
);

-- Step 3: Call procedure
CALL datascience_public_misc.eclipse_analytics.update_dex_stats();

-- Step 4: Define procedure that does full reload
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_dex_stats()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Truncate existing data
    TRUNCATE TABLE datascience_public_misc.eclipse_analytics.dex_stats;
    
    -- Insert fresh data
    INSERT INTO datascience_public_misc.eclipse_analytics.dex_stats (
        dex_name,
        swappers,
        swap_txs,
        amount_usd
    )
    WITH tokens AS (
        SELECT * FROM ( VALUES
            ('USDC', 'USD Coin', 'AKEWE7Bgh87GPp171b4cJPSSZfmZwQ3KaqYqXoKLNAEE'),
            ('SOL', 'Solana', 'BeRUj3h7BqkbdfFU7FBNYbodgf8GCHodzKvF9aVjNNfL'),
            ('WIF', 'DogWifHat', '841P4tebEgNux2jaWSjCoi9LhrVr9eHGjLc758Va3RPH'),
            ('ETH', 'Turbo ETH', 'GU7NS9xCwgNPiAdJ69iusFrRfawjDDPjeMBovhV1d4kn'),
            ('ORCA', 'Orca', '2tGbYEm4nuPFyS6zjDTELzEhvVKizgKewi6xT7AaSKzn'),
            ('USDT', 'Tether USD', 'CEBP3CqAbW4zdZA57H2wfaSG1QNdzQ72GiQEbQXyW9Tm'),
            ('TIA', 'Celestia', '9RryNMhAVJpAwAGjCAMKbbTFwgjapqPkzpGMfTQhEjf8'),
            ('STTIA', 'Stride Staked TIA', 'V5m1Cc9VK61mKL8xVYrjR7bjD2BC5VpADLa6ws3G8KM'),
            ('WEETHS', 'Super Symbiotic LRT', 'F72PqK74jc28zjC7kWDk6ykJ2ZAbjNzn2jaAY9v9M6om'),
            ('WBTC', 'Wrapped BTC', '7UTjr1VC6Z9DPsWD6mh5wPzNtufN17VnzpKS3ASpfAji'),
            ('ETH', 'Ethereum', 'So11111111111111111111111111111111111111112')
        ) AS t(symbol, name, mint)
    ),
    dexs AS (
        SELECT * FROM ( VALUES
            ('LIFINITY', '4UsSbJQZJTfZDFrgvcPBRCSg5BbcQE6dobnriCafzj12'),
            ('INVARIANT', 'iNvTyprs4TX8m6UeUEkeqDFjAL9zRCRWcexK9Sd4WEU'),
            ('ORCA', 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc')
        ) AS t(dex_name, program_id)
    ),
    time_filter AS (
        SELECT dateadd('day', -1000, CURRENT_DATE()) as start_date
    ),
    price_cte AS (
        SELECT
            hour,
            symbol as price_symbol,
            token_address,
            price
        FROM crosschain.price.ez_prices_hourly
        WHERE token_address IN (
            'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', -- USDC
            'So11111111111111111111111111111111111111112', -- SOL 
            'EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm', -- WIF
            '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs', -- ETH
            'orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE', -- ORCA
            'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', -- USDT
            'ibc/D79E7D83AB399BFFF93433E54FAA480C191248FC556924A2A8351AE2638B3877', -- TIA
            'ibc/698350B8A61D575025F3ED13E9AC9C0F45C89DEFE92F76D5838F1D3C1A7FF7C9', -- STTIA
            '0x917cee801a67f933f2e6b33fc0cd1ed2d5909d88', -- WEETHS
            '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh' -- WBTC
        )
        AND hour >= (select start_date from time_filter)
    ),
    valid_swap_txs AS (
        SELECT DISTINCT 
            tx.tx_id,
            tx.block_timestamp,
            tx.signers[0] as swapper,
            tx.succeeded
        FROM eclipse.core.fact_transactions tx,
        LATERAL FLATTEN (input => log_messages) logs,
        time_filter tf
        WHERE tx.block_timestamp >= (select start_date from time_filter)
        AND tx.succeeded = true
        AND REGEXP_LIKE(logs.value, 'Program log: Instruction: (TwoHopSwap|TwoHopSwapV2|Swap|SwapV2)')
    ),
    swap_events AS (
        SELECT 
            e.tx_id,
            e.block_timestamp,
            e.program_id,
            d.dex_name
        FROM eclipse.core.fact_events e
        INNER JOIN valid_swap_txs v ON e.tx_id = v.tx_id and v.block_timestamp = e.block_timestamp
        INNER JOIN dexs d ON e.program_id = d.program_id
        WHERE e.program_id IN (
            '4UsSbJQZJTfZDFrgvcPBRCSg5BbcQE6dobnriCafzj12',
            'iNvTyprs4TX8m6UeUEkeqDFjAL9zRCRWcexK9Sd4WEU',
            'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        )
    ),
    transfers_with_context AS (
        SELECT 
            v.block_timestamp,
            v.tx_id,
            v.swapper,
            t.index,
            t.inner_index,
            t.mint,
            t.amount,
            se.dex_name,
            tok.symbol,
            p.price,
            t.amount / pow(10, COALESCE(decimal, 9)) as swap_amount,
            swap_amount * price as amount_usd
        FROM valid_swap_txs v
        INNER JOIN eclipse.core.fact_transfers t ON v.tx_id = t.tx_id and v.block_timestamp = t.block_timestamp
        INNER JOIN swap_events se ON v.tx_id = se.tx_id and v.block_timestamp = se.block_timestamp
        LEFT JOIN tokens tok ON t.mint = tok.mint
        LEFT JOIN price_cte p ON (
            date_trunc('hour', v.block_timestamp) = p.hour 
            AND tok.symbol = p.price_symbol
        )
    ),
    outflows AS (
        SELECT 
            *,
            'Outflow' as type
        FROM transfers_with_context 
        WHERE inner_index = 0
    ),
    inflows AS (
        SELECT 
            *,
            'Inflow' as type
        FROM transfers_with_context
        QUALIFY ROW_NUMBER() OVER (PARTITION BY index, tx_id ORDER BY inner_index DESC) = 1
    ),
    base_swap AS (
        SELECT * FROM outflows
        UNION ALL
        SELECT * FROM inflows
    ),
    swap_agg AS (
        SELECT 
            block_timestamp,
            swapper,
            MIN(dex_name) as dex_name,
            MIN(CASE WHEN type = 'Outflow' THEN symbol END) as outflow_symbol,
            SUM(CASE WHEN type = 'Outflow' THEN swap_amount END) as outflow_amount,
            SUM(CASE WHEN type = 'Outflow' THEN amount_usd END) as outflow_usd,
            MIN(CASE WHEN type = 'Inflow' THEN symbol END) as inflow_symbol,
            SUM(CASE WHEN type = 'Inflow' THEN swap_amount END) as inflow_amount,
            SUM(CASE WHEN type = 'Inflow' THEN amount_usd END) as inflow_usd,
            MIN(CASE WHEN type = 'Outflow' THEN mint END) as outflow_mint,
            MIN(CASE WHEN type = 'Inflow' THEN mint END) as inflow_mint,
            tx_id
        FROM base_swap
        GROUP BY block_timestamp, swapper, tx_id
    )
    SELECT 
        dex_name,
        COUNT(DISTINCT swapper) as swappers,
        COUNT(DISTINCT tx_id) as swap_txs,
        SUM(COALESCE(outflow_usd, inflow_usd)) as amount_usd
    FROM swap_agg
    GROUP BY 1
    ORDER BY amount_usd DESC;

    RETURN 'Eclipse DEX stats updated successfully';
END;
$$;

-- Create task to update DEX stats every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_dex_stats_task
    WAREHOUSE = 'DATA_SCIENCE'
    SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
    CALL datascience_public_misc.eclipse_analytics.update_dex_stats();

-- Resume the task
ALTER TASK datascience_public_misc.eclipse_analytics.update_dex_stats_task RESUME;

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.dex_stats TO ROLE INTERNAL_DEV;

-- Individual access 
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.dex_stats TO ROLE VELOCITY_ETHEREUM;