/*
tx fee of tx where qualified receiver receives NEAR from sweat welcome 
 */

select
date_trunc('day', block_timestamp) as day_, 
SUM(transaction_fee/1e24) as total_tx_fee
from near.core.fact_transactions 
inner join datascience_public_misc.near_analytics.qualified_sweat_users 
on tx_receiver = sweat_receiver 
    and block_timestamp = first_receive_timestamp 
    and is_first_sweat_receive = 1
group by day_;


