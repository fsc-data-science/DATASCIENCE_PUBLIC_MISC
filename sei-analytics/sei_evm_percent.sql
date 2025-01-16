select 
date_trunc('month', block_timestamp) as month_,
count(distinct attribute_value) as n_evm_addr
 from sei.core.fact_msg_attributes
where msg_type = 'signer'
and attribute_key = 'evm_addr'
and block_timestamp >= '2024-06-01'
group by month_
order by month_ desc;



;

with daily_signers AS (
select date_trunc('month', block_timestamp) as month_,
attribute_key, 
count(distinct tx_id) as n_tx -- tx id, attribute key is safely unique 
from sei.core.fact_msg_attributes 
where block_timestamp >= '2024-01-01'
and msg_type = 'signer'
group by month_, attribute_key
order by month_ desc
),

daily_evms AS (   
select 
a.month_,
a.n_tx,
b.n_tx as n_evm_tx,
n_evm_tx/a.n_tx as evm_percent 
from daily_signers a left join daily_signers b on a.month_ = b.month_
where a.attribute_key = 'sei_addr'
and b.attribute_key = 'evm_addr'
),

unique_tx_overall AS (
select date_trunc('month', block_timestamp) as month_,
count(distinct tx_id) as n_overall_tx  
from sei.core.fact_transactions 
where block_timestamp >= '2024-01-01'
group by month_
)
    
select u.month_,
u.n_overall_tx,
d.n_tx,
d.n_evm_tx,
d.evm_percent
from unique_tx_overall u
left join daily_evms d on u.month_ = d.month_
;

