-- NEAR blockchain
-- net change is the total supply
-- the balance of the treasury is authorized but not circulating 
WITH authorized_supply AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(DIV0(args:amount::float, 1e6)) as authorized_amount
            FROM near.core.fact_actions_events_function_call
            WHERE method_name = 'mint'
                AND receipt_succeeded = TRUE
                AND signer_id = '867487d0e6545dc3e34824bbb8213a8b82b8b6248322f509628f68b73ace74e2'
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdt_supply)),
                    '2023-06-30'::DATE
                )
            GROUP BY 1
        ),
        
        treasury_flows AS (
            SELECT 
                date_trunc('day', block_timestamp)::DATE as day_,
                SUM(CASE WHEN to_address = 'tether-treasury.near' THEN amount ELSE 0 END) as treasury_inflow,
                SUM(CASE WHEN from_address = 'tether-treasury.near' THEN amount ELSE 0 END) as treasury_outflow
            FROM near.core.ez_token_transfers
            WHERE (to_address = 'tether-treasury.near' OR from_address = 'tether-treasury.near')
                AND contract_address = 'usdt.tether-token.near'
                AND date_trunc('day', block_timestamp)::DATE >= COALESCE(
                    DATEADD(day, -2, (SELECT MAX(day_) FROM datascience_public_misc.near_analytics.near_daily_usdt_supply)),
                    '2023-06-30'::DATE
                )
            GROUP BY 1
        ),
        
        combined_metrics AS (
            SELECT 
                COALESCE(a.day_, t.day_) as day_,
                COALESCE(a.authorized_amount, 0) as authorized_amount,
                COALESCE(t.treasury_inflow, 0) as treasury_inflow,
                COALESCE(t.treasury_outflow, 0) as treasury_outflow
            FROM authorized_supply a
            FULL OUTER JOIN treasury_flows t 
                ON a.day_ = t.day_
            WHERE COALESCE(a.authorized_amount, 0) != 0 
               OR COALESCE(t.treasury_inflow, 0) != 0
               OR COALESCE(t.treasury_outflow, 0) != 0
        )
        
        SELECT * FROM combined_metrics
    ) AS source
    ON target.day_ = source.day_
    WHEN MATCHED THEN
        UPDATE SET 
            target.authorized_amount = source.authorized_amount,
            target.treasury_inflow = source.treasury_inflow,
            target.treasury_outflow = source.treasury_outflow
    WHEN NOT MATCHED THEN
        INSERT (day_, authorized_amount, treasury_inflow, treasury_outflow)
        VALUES (
            source.day_,
            source.authorized_amount,
            source.treasury_inflow,
            source.treasury_outflow
        );

    RETURN 'NEAR daily USDT supply metrics updated successfully';




-- Tether Treasury 0x5754284f345afc66a98fbB0a0Afe71e0F007B949 
-- USDT 0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7

select from_address, to_address, count(1) as n_ 
from avalanche.core.fact_transactions
where (
    to_address = '0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7'
or from_address = lower('0x5754284f345afc66a98fbB0a0Afe71e0F007B949')
)
and block_timestamp < '2022-01-01'
group by 1,2
;

with mint_burn_events as (
select 
block_timestamp,
tx_hash,
case when from_address = lower('0x0000000000000000000000000000000000000000') then 'mint' else 'burn' end as event_,
case when from_address = lower('0x0000000000000000000000000000000000000000') then amount else -amount end as amount_signed
 from avalanche.core.ez_token_transfers
where contract_address = lower('0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7')
and (
    from_address = '0x0000000000000000000000000000000000000000'
or to_address = '0x0000000000000000000000000000000000000000'
)
)

select date_trunc('day', block_timestamp) as day_,
sum(case when event_ = 'mint' then amount_signed else 0 end) as amount_mint,
sum(case when event_ = 'burn' then amount_signed else 0 end) as amount_burn,
sum(amount_signed) as net_change,
sum(net_change) over (order by day_ asc) as usdt_in_circulation
from mint_burn_events
group by day_
order by day_ desc
;



;
/*
0xd3b62ad16d634f186e5d81d314f10efd8c628bc3 
    0x503560430e4b5814dda09ac789c3508bb41b24b2 -- tether deployer received first mint 
0xaf2c57f6bb32dcc52d8c296bf5d46bca2a69f580
0xD83d5C96BfB9e5F890E8Be48165b13dDB0eCd2Aa -- tether multi-sig has burnt 500M 
 */