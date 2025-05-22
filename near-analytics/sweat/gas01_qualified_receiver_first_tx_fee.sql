
/*
tx fee of first tx where qualified receiver is tx signer 
 */

with qualified_first_sign AS (
select
tx_signer, 
min(block_timestamp) as first_sign_timestamp
from near.core.fact_transactions 
inner join datascience_public_misc.near_analytics.qualified_sweat_users 
on tx_signer = sweat_receiver 
    and is_first_sweat_receive = 1
group by tx_signer
),

first_sign_tx_fee AS (
-- just in case two tx signed in same block
-- average the fees. Only 1 address has ever done this and it was before 2023.
select 
block_timestamp, 
ft.tx_signer,
avg(transaction_fee/1e24) as avg_tx_fee,
count(tx_hash) as tx_count_in_block
from near.core.fact_transactions ft
inner join qualified_first_sign qfs
on ft.tx_signer = qfs.tx_signer 
    and ft.block_timestamp = qfs.first_sign_timestamp
group by block_timestamp, ft.tx_signer
)

select date_trunc('day', block_timestamp) as day_, 
sum(avg_tx_fee) as total_tx_fee
from first_sign_tx_fee
group by day_;



