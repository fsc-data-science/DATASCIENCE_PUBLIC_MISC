WITH mint_burn_events AS (
    SELECT 
        block_timestamp,
        tx_hash,
        CASE 
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN 'mint' 
            ELSE 'burn' 
        END as event_,
        CASE 
            WHEN from_address = '0x0000000000000000000000000000000000000000' THEN amount 
            ELSE -amount 
        END as amount_signed
    FROM avalanche.core.ez_token_transfers
    WHERE contract_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7'
        AND (from_address = '0x0000000000000000000000000000000000000000'
             OR to_address = '0x0000000000000000000000000000000000000000')
),
daily_mint_burn AS (
    SELECT 
        date_trunc('day', block_timestamp) as day_,
        SUM(CASE WHEN event_ = 'mint' THEN amount_signed ELSE 0 END) as amount_mint,
        SUM(CASE WHEN event_ = 'burn' THEN amount_signed ELSE 0 END) as amount_burn,
        SUM(amount_signed) as net_change
    FROM mint_burn_events
    GROUP BY 1
),
treasury_flows AS (
    SELECT 
        date_trunc('day', block_timestamp) as day_,
        SUM(CASE 
            WHEN to_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
            THEN amount ELSE 0 END) as treasury_inflow,
        SUM(CASE 
            WHEN from_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
            THEN amount ELSE 0 END) as treasury_outflow
    FROM avalanche.core.ez_token_transfers
    WHERE (to_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949' 
           OR from_address = '0x5754284f345afc66a98fbb0a0afe71e0f007b949')
        AND contract_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7'
    GROUP BY 1
), 

daily_supply as (
SELECT 
    COALESCE(m.day_, t.day_) as day_,
    COALESCE(m.amount_mint, 0) as amount_mint,
    COALESCE(m.amount_burn, 0) as amount_burn,
    COALESCE(m.net_change, 0) as net_change,
    COALESCE(t.treasury_inflow, 0) as treasury_inflow,
    COALESCE(t.treasury_outflow, 0) as treasury_outflow,
        SUM(COALESCE(m.net_change, 0)) OVER (ORDER BY COALESCE(m.day_, t.day_) ASC) as total_authorized_supply,
    SUM(COALESCE(t.treasury_inflow, 0)) OVER (ORDER BY COALESCE(m.day_, t.day_) ASC) as cumulative_treasury_inflow,
    SUM(COALESCE(t.treasury_outflow, 0)) OVER (ORDER BY COALESCE(m.day_, t.day_) ASC) as cumulative_treasury_outflow
FROM daily_mint_burn m
FULL JOIN treasury_flows t 
    ON m.day_ = t.day_
ORDER BY day_ DESC
)
select 
*, cumulative_treasury_inflow - cumulative_treasury_outflow as treasury_balance,
total_authorized_supply - treasury_balance as circulating_supply
from daily_supply;

