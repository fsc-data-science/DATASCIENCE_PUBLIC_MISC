select * from datascience_public_misc.eclipse_analytics.partial_ez_swaps limit 5;

-- Step 1: Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS datascience_public_misc.eclipse_analytics;

-- Step 2: Create table based on observation level (trade-level/transaction-level swap data)
CREATE OR REPLACE TABLE datascience_public_misc.eclipse_analytics.partial_ez_swaps (
    block_timestamp TIMESTAMP,
    tx_id VARCHAR,
    swapper VARCHAR,
    platform VARCHAR,
    mint_out VARCHAR,
    symbol_out VARCHAR,
    amount_out FLOAT,
    amount_out_usd FLOAT,
    mint_in VARCHAR,
    symbol_in VARCHAR,
    amount_in FLOAT,
    amount_in_usd FLOAT
);

-- Step 3: Call the procedure
CALL datascience_public_misc.eclipse_analytics.update_partial_ez_swaps();

-- Step 4: Define the procedure with a 3-day lookback period
CREATE OR REPLACE PROCEDURE datascience_public_misc.eclipse_analytics.update_partial_ez_swaps()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
MERGE INTO datascience_public_misc.eclipse_analytics.partial_ez_swaps AS target
USING (
    -- Trade-level swap data with lookback period
    with tokens as (
      select * from ( values
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
      ) as t(symbol, name, mint)
    )
    , dexs as (
      select * from ( values
        ('LIFINITY', '4UsSbJQZJTfZDFrgvcPBRCSg5BbcQE6dobnriCafzj12'),
        ('INVARIANT', 'iNvTyprs4TX8m6UeUEkeqDFjAL9zRCRWcexK9Sd4WEU'),
        ('ORCA', 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc')
      ) as t(dex_name, program_id)
    )
    , price_cte as (
        select
          hour,
          symbol as price_symbol,
          token_address,
          price
        from crosschain.price.ez_prices_hourly
        where token_address in (
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
        and hour >= DATEADD(day, -3, 
            COALESCE((SELECT MAX(block_timestamp) FROM datascience_public_misc.eclipse_analytics.partial_ez_swaps), 
                     '2020-01-01')
        )
    )
    , base as (
    select distinct
      t.block_timestamp,
      t.tx_id,
      t.index,
      t.inner_index,
      t.mint,
      t.amount / pow(10, decimal) as swap_amount,
      swap_amount * price as amount_usd,
      iff(name = 'Turbo ETH', 'tETH', price_symbol) as symbol,
      dex_name,
      t.tx_from,
      t.tx_to,
      tx.signers[0] as swapper
    from eclipse.core.fact_events e
    join (select tx_id as logs_tx_id, replace(logs.value, 'Program log: Instruction: ', '') as instructions
      from eclipse.core.fact_transactions tx, lateral flatten (input => log_messages) logs
      where replace(logs.value, 'Program log: Instruction: ', '') in 
        ('TwoHopSwap', 'TwoHopSwapV2', 'Swap', 'SwapV2')
        and tx.block_timestamp >= DATEADD(day, -3, 
            COALESCE((SELECT MAX(block_timestamp) FROM datascience_public_misc.eclipse_analytics.partial_ez_swaps), 
                     '2020-01-01')
        )
      ) l on (e.tx_id = l.logs_tx_id)
    join eclipse.core.fact_transfers t on t.tx_id = e.tx_id
    join eclipse.core.fact_transactions tx on tx.tx_id = e.tx_id
    left join tokens on tokens.mint = t.mint
    join dexs on dexs.program_id = e.program_id
    left join price_cte on (date_trunc(hour, t.block_timestamp) = price_cte.hour and tokens.symbol = price_cte.price_symbol)
    where e.program_id in ('4UsSbJQZJTfZDFrgvcPBRCSg5BbcQE6dobnriCafzj12', 
          'iNvTyprs4TX8m6UeUEkeqDFjAL9zRCRWcexK9Sd4WEU', 
          'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc')
    and e.block_timestamp >= DATEADD(day, -3, 
        COALESCE((SELECT MAX(block_timestamp) FROM datascience_public_misc.eclipse_analytics.partial_ez_swaps), 
                 '2020-01-01')
    )
    and t.block_timestamp >= DATEADD(day, -3, 
        COALESCE((SELECT MAX(block_timestamp) FROM datascience_public_misc.eclipse_analytics.partial_ez_swaps), 
                 '2020-01-01')
    )
    and tx.block_timestamp >= DATEADD(day, -3, 
        COALESCE((SELECT MAX(block_timestamp) FROM datascience_public_misc.eclipse_analytics.partial_ez_swaps), 
                 '2020-01-01')
    )
    )
    , outflows as (
      select *,
        'Outflow' as type
      from base where inner_index = 0
    )
    , inflows as ( -- last transfer for each index
    select *,
      'Inflow' as type
    from base 
    qualify row_number() over (partition by index, tx_id order by inner_index desc) = 1
    )
    , base_swap as (
    select * from outflows
    UNION
    select * from inflows
    )
    , trade_level as (
    select 
      block_timestamp,
      tx_id,
      swapper,
      min(case when type = 'Outflow' then dex_name end) as platform,
      min(case when type = 'Outflow' then mint end) as mint_out,
      min(case when type = 'Outflow' then symbol end) as symbol_out,
      sum(case when type = 'Outflow' then swap_amount end) as amount_out,
      sum(case when type = 'Outflow' then amount_usd end) as amount_out_usd,
      min(case when type = 'Inflow' then mint end) as mint_in,
      min(case when type = 'Inflow' then symbol end) as symbol_in,
      sum(case when type = 'Inflow' then swap_amount end) as amount_in,
      sum(case when type = 'Inflow' then amount_usd end) as amount_in_usd
    from base_swap
    group by block_timestamp, tx_id, swapper
    )

    -- Final output with all requested fields
    select 
      block_timestamp,
      tx_id,
      swapper,
      platform,
      mint_out,
      symbol_out,
      amount_out,
      amount_out_usd,
      mint_in,
      symbol_in,
      amount_in,
      amount_in_usd
    from trade_level
) AS source
ON target.tx_id = source.tx_id
WHEN MATCHED THEN
    UPDATE SET 
        target.block_timestamp = source.block_timestamp,
        target.swapper = source.swapper,
        target.platform = source.platform,
        target.mint_out = source.mint_out,
        target.symbol_out = source.symbol_out,
        target.amount_out = source.amount_out,
        target.amount_out_usd = source.amount_out_usd,
        target.mint_in = source.mint_in,
        target.symbol_in = source.symbol_in,
        target.amount_in = source.amount_in,
        target.amount_in_usd = source.amount_in_usd
WHEN NOT MATCHED THEN
    INSERT (
        block_timestamp,
        tx_id,
        swapper,
        platform,
        mint_out,
        symbol_out,
        amount_out,
        amount_out_usd,
        mint_in,
        symbol_in,
        amount_in,
        amount_in_usd
    )
    VALUES (
        source.block_timestamp,
        source.tx_id,
        source.swapper,
        source.platform,
        source.mint_out,
        source.symbol_out,
        source.amount_out,
        source.amount_out_usd,
        source.mint_in,
        source.symbol_in,
        source.amount_in,
        source.amount_in_usd
    );

RETURN 'Eclipse Partial EZ Swaps updated successfully';
END;
$$;

-- Step 5: Add clustering to the table for improved query performance
ALTER TABLE datascience_public_misc.eclipse_analytics.partial_ez_swaps
CLUSTER BY (block_timestamp);

-- Step 6: Create a scheduled task to run every 12 hours
CREATE OR REPLACE TASK datascience_public_misc.eclipse_analytics.update_partial_ez_swaps_task
  WAREHOUSE = 'DATA_SCIENCE'
  SCHEDULE = 'USING CRON 0 */12 * * * America/Los_Angeles'
AS
  CALL datascience_public_misc.eclipse_analytics.update_partial_ez_swaps();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK datascience_public_misc.eclipse_analytics.update_partial_ez_swaps_task RESUME;

-- Step 7: Set appropriate permissions
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE INTERNAL_DEV;
GRANT ALL PRIVILEGES ON TABLE datascience_public_misc.eclipse_analytics.partial_ez_swaps TO ROLE INTERNAL_DEV;

-- Individual access for VELOCITY_ETHEREUM role
GRANT USAGE ON DATABASE datascience_public_misc TO ROLE VELOCITY_ETHEREUM;
GRANT USAGE ON SCHEMA datascience_public_misc.eclipse_analytics TO ROLE VELOCITY_ETHEREUM;
GRANT SELECT ON TABLE datascience_public_misc.eclipse_analytics.partial_ez_swaps TO ROLE VELOCITY_ETHEREUM;