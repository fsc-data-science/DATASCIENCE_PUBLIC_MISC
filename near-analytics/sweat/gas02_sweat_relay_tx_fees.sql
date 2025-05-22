select 
date_trunc('day', block_timestamp) as day_, 
sum(transaction_fee/1e24) as total_tx_fee
from near.core.fact_transactions
where tx_signer = 'sweat-relayer.near'
group by day_;
