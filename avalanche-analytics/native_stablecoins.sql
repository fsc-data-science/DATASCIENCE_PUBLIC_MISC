WITH avax_native_stablecoins AS (
  SELECT * FROM (
    VALUES 
      ('Circle USDC', 'USDC', '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e'),
      ('Tether', 'USDT', '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7')
  ) AS t (name, symbol, token_address)
);

select lower('0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7') from dual;

select * from avalanche.core.fact_transactions
where to_address in (select token_address from avax_native_stablecoins)
 and block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
limit 10
;

WITH avax_native_stablecoins AS (
  SELECT * FROM (
    VALUES 
      ('Circle USDC', 'USDC', '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e'),
      ('Tether', 'USDT', '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7')
  ) AS t (name, symbol, token_address)
),
events_summary AS (
  SELECT 
    ans.name,
    ans.symbol,
    el.event_name,
    COUNT(*) as event_count,
    min(block_timestamp) as first_event_timestamp
  FROM avalanche.core.ez_decoded_event_logs el
  JOIN avax_native_stablecoins ans 
    ON el.contract_address = ans.token_address
  WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
  GROUP BY 1,2,3
  ORDER BY 1,2,4 DESC
)
SELECT *
FROM events_summary;

SELECT 
  event_name,
  decoded_log,
  COUNT(*) as event_count
FROM avalanche.core.ez_decoded_event_logs
WHERE contract_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7' -- USDT on Avalanche
  AND block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
  AND event_name IN ('Mint', 'Burn')
GROUP BY 1, 2
;

-- Tether Treasury 0x5754284f345afc66a98fbB0a0Afe71e0F007B949 

