with near_sends AS (
select t.block_timestamp, block_id, t.tx_hash, 
from_address, to_address, transfer_type, 'out' as direction_,
contract_address, t.symbol, t.amount
from 
  near.core.ez_token_transfers t 
        inner join datascience_public_misc.near_analytics.sweat_welcome_transfers w 
            on t.from_address = w.sweat_receiver 
where t.block_timestamp > current_date - 10
),

near_receives AS (
select t.block_timestamp, block_id, t.tx_hash, 
from_address, to_address, transfer_type,  'in' as direction_,
contract_address, t.symbol, t.amount
from 
  near.core.ez_token_transfers t 
        inner join datascience_public_misc.near_analytics.sweat_welcome_transfers w 
            on t.to_address = w.sweat_receiver 
where t.block_timestamp > current_date - 10
)

select * from near_sends 
UNION ALL 
select * from near_receives;



with near_sends AS (
select t.block_timestamp, block_id, t.tx_hash, 
from_address, to_address, transfer_type, 'out' as direction_,
contract_address, t.symbol, t.amount
from near.core.ez_token_transfers t 
where t.from_address in ('971abc7c7aa4741afba9ff22ae32034bb45b098ecedd9f7e1fc61847ffb076a0',
    '0a0b2f912ffbc0e69aed7160d53c75b109ecefabeb3c4f0b64fa5e7b51f17c01',
    '86311693397c03de0a8465b6cc80664e0a2832b8c7216cdc97c2864316a96dd2')
),

near_receives AS (
select t.block_timestamp, block_id, t.tx_hash, 
from_address, to_address, transfer_type, 'in' as direction_,
contract_address, t.symbol, t.amount
from near.core.ez_token_transfers t 
where t.to_address in ('971abc7c7aa4741afba9ff22ae32034bb45b098ecedd9f7e1fc61847ffb076a0',
    '0a0b2f912ffbc0e69aed7160d53c75b109ecefabeb3c4f0b64fa5e7b51f17c01',
    '86311693397c03de0a8465b6cc80664e0a2832b8c7216cdc97c2864316a96dd2')
),

near_fees_paid AS (
    select tx_signer as user_, sum(DIV0(transaction_fee, 1e24)) as total_fees
    from near.core.fact_transactions 
    where tx_signer in ('971abc7c7aa4741afba9ff22ae32034bb45b098ecedd9f7e1fc61847ffb076a0',
    '0a0b2f912ffbc0e69aed7160d53c75b109ecefabeb3c4f0b64fa5e7b51f17c01',
    '86311693397c03de0a8465b6cc80664e0a2832b8c7216cdc97c2864316a96dd2')
    group by tx_signer
), 

select_users AS (
select * from near_sends 
UNION ALL 
select * from near_receives
),

net_transfers AS (
select 
contract_address, symbol,
case when direction_ = 'out' then from_address else to_address end as user_,
sum(case when direction_ = 'out' then -amount else amount end) as amount,
from select_users 
group by contract_address, symbol, user_
)

select 
user_, 
contract_address, symbol,
amount, 
case when contract_address = 'wrap.near' then COALESCE(total_fees, 0) else 0 end as fees_paid,
case when contract_address = 'wrap.near' then amount - fees_paid else amount end as amount_net_fees
from net_transfers left join near_fees_paid using (user_)
;

select 
block_timestamp, 
block_id, tx_hash, 
tx_signer, div0(transaction_fee, 1e24) as transaction_fee
 from near.core.fact_transactions 
where tx_signer = '971abc7c7aa4741afba9ff22ae32034bb45b098ecedd9f7e1fc61847ffb076a0'
and block_timestamp >= '2023-01-01'

