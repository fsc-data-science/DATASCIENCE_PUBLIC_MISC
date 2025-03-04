WITH sweat_users AS (
    SELECT sweat_receiver, min(block_timestamp) as first_receive_date
    FROM datascience_public_misc.near_analytics.sweat_welcome_transfers
    group by sweat_receiver
),

daily_swap_based_prices AS (
select 
date_trunc('day', block_timestamp) as day_,
token_out_contract,
symbol_out,
AVG( DIV0(coalesce(amount_out_usd, amount_in_usd), amount_out) ) as token_price_usd
from near.defi.ez_dex_swaps 
where 
token_out_contract in (
    'wrap.near',
   'token.sweat',
    '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
    'token.burrow.near',
    'meta-pool.near',
    'usdt.tether-token.near',
    'linear-protocol.near',
    'aurora'
  )
group by day_, token_out_contract, symbol_out
),

daily_token_transfers AS (
  SELECT 
    DATE_TRUNC('day', block_timestamp) as day_,
    contract_address,
    symbol,
    sum(amount) as token_volume,
    sum(amount_usd) as total_usd_volume
  FROM near.core.ez_token_transfers t
  INNER JOIN sweat_users s ON t.from_address = s.sweat_receiver
where contract_address IN (
    'wrap.near',
   'token.sweat',
    '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1', -- USDC 
    'token.burrow.near',
    'meta-pool.near',
    'usdt.tether-token.near',
    'linear-protocol.near',
    'aurora'
  )
  GROUP BY day_, contract_address, symbol
),

daily_transfers_infill AS (
 SELECT 
    t.day_,
    contract_address,
    symbol,
    token_volume,
    coalesce(total_usd_volume, token_volume * token_price_usd) as total_usd_volume
from daily_token_transfers t left join daily_swap_based_prices p on t.day_ = p.day_ and t.contract_address = p.token_out_contract and t.symbol = p.symbol_out 
),

near_daily_price AS (
  SELECT 
    DATE_TRUNC('day', hour) as day_,
    AVG(price) as avg_near_price
  FROM near.price.ez_prices_hourly
  WHERE is_native = true
  GROUP BY 1
),

daily_near_denominated_transfer_volume AS (
SELECT 
  t.day_,
  t.contract_address,
  t.symbol,
  t.token_volume,
  t.total_usd_volume,
  p.avg_near_price,
  t.total_usd_volume / NULLIF(p.avg_near_price, 0) as volume_in_near
FROM daily_transfers_infill t
LEFT JOIN near_daily_price p ON t.day_ = p.day_
)

select date_trunc('month', day_) as month_, 
contract_address,
symbol, 
sum(token_volume) as token_volume,
sum(total_usd_volume) as total_usd_volume,
sum(volume_in_near) as volume_in_near
from daily_near_denominated_transfer_volume
group by 1,2,3
order by month_ desc, volume_in_near desc


