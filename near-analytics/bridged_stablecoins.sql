/*
Dropping for now. Bridge net transfers yields negative circulation,
Implying minting process not accounted for. 

*/


WITH bridged_tokens AS (
  SELECT * FROM (VALUES
    ('a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 'bridged USDC'),
    ('853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near', 'FRAX'),
    ('dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 'bridged USDT')
  ) AS t (contract_address, token_name)
)
SELECT 
    b.token_name,
    t.contract_address,
    MIN(t.block_timestamp) as first_transfer,
    MIN(block_id) as first_block_id,
    COUNT(*) as total_transfers
FROM near.core.ez_token_transfers t
JOIN bridged_tokens b ON t.contract_address = b.contract_address
GROUP BY 1,2
ORDER BY first_transfer;


-- with stable_bridge_flows as (
select 
block_timestamp, 
block_id, 
tx_hash, 
token_address, 
symbol, 
direction, 
bridge_address,
amount,
IFNULL(source_address, 'mint') as source_,
IFNULL(destination_address, 'burn') as dest_,
source_chain, 
destination_chain
from near.defi.ez_bridge_activity 
where token_address 
IN (
    'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
    '853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near',
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near') 
and receipt_succeeded = TRUE
)
;

WITH stable_bridge_flows AS (
  SELECT 
    DATE_TRUNC('day', block_timestamp) as date,
    token_address,
    symbol,
    SUM(CASE 
      WHEN direction = 'inbound' THEN amount 
      WHEN direction = 'outbound' THEN -amount 
    END) as daily_net_flow
  FROM near.defi.ez_bridge_activity 
  WHERE token_address IN (
    'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
    '853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near',
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
  ) 
  AND receipt_succeeded = TRUE
  GROUP BY 1,2,3
),

running_circulation AS (
  SELECT 
    date,
    token_address,
    symbol,
    daily_net_flow,
    SUM(daily_net_flow) OVER (
      PARTITION BY token_address, symbol  
      ORDER BY date asc
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_circulation
  FROM stable_bridge_flows
)

SELECT * 
FROM running_circulation
ORDER BY token_address, date DESC;



;



;
WITH bridged_tokens AS (
  SELECT * FROM (VALUES
    ('a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 'bridged USDC'),
    ('853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near', 'FRAX'),
    ('dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 'bridged USDT')
  ) AS t (contract_address, token_name)
)
SELECT 
    b.token_name,
    t.contract_address,
    t.block_timestamp as first_transfer_time,
    t.tx_hash as first_tx_hash,
    t.from_address,
    t.to_address,
    t.amount,
    t.amount_usd
FROM near.core.ez_token_transfers t
JOIN bridged_tokens b ON t.contract_address = b.contract_address
where block_timestamp < '2022-01-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.contract_address ORDER BY t.block_timestamp ASC) = 1
ORDER BY first_transfer_time
;

;
select 
block_timestamp, block_id, 
tx_hash, action_id, 
signer_id, 
predecessor_id,
receiver_id,
action_name, method_name, 
args
from near.core.fact_actions_events_function_call 
where block_timestamp >= '2021-03-30'
and block_timestamp <= '2022-01-01'
and receipt_succeeded = TRUE
and (
predecessor_id IN (
    'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
    '853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near',
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near') 
OR receiver_id IN (
    'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
    '853d955acef822db058eb8505911ed77f175b99e.factory.bridge.near',
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near')
)
limit 500;