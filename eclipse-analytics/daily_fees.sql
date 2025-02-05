 SELECT 
 tx_id, instruction:data,
 substr(instruction:data, 0, 1) as first_byte,
livequery.utils.udf_base58_to_hex(e.instruction:data) as data_hex,
 substr(livequery.utils.udf_base58_to_hex(e.instruction:data), 3, 2) as is_priority 
 FROM eclipse.core.fact_events e 
            WHERE e.program_id = 'ComputeBudget111111111111111111111111111111'
            and block_timestamp >= current_date - 1
limit 1000
;

with main_tx AS (
    select  
date_trunc('day', block_timestamp) as day_,
case when instructions[0]:programId = 'ComputeBudget111111111111111111111111111111' then 'compute-tx' else 'other-tx' end as tx_type,
case when substr(instructions[0]:data, 0, 1) = '3' then 'priority' else 'legacy' end as fee_type, 
DIV0(sum(fee), 1e9) as fee
from eclipse.core.fact_transactions
group by day_, tx_type, fee_type
),

vote_summary AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as day_,
        COUNT(DISTINCT tx_id) as n_tx,
        n_tx * 0.00000005 as vote_fees  -- Fixed lamport cost per vote
    FROM eclipse.gov.fact_votes
    group by day_

)

select 
m.day_,
sum(case when m.tx_type = 'compute-tx' and m.fee_type = 'priority' then m.fee else 0 end) as compute_tx_priority_fee,
sum(case when m.tx_type = 'other-tx' and m.fee_type = 'priority' then m.fee else 0 end) as other_tx_priority_fee,
sum(case when m.tx_type = 'compute-tx' and m.fee_type = 'legacy' then m.fee else 0 end) as compute_tx_legacy_fee,
sum(case when m.tx_type = 'other-tx' and m.fee_type = 'legacy' then m.fee else 0 end) as other_tx_legacy_fee,
coalesce(v.vote_fees, 0) as vote_fees
from main_tx m
left join vote_summary v on m.day_ = v.day_
group by m.day_, v.vote_fees
;



select 
tx_id, instructions[0],
instructions[0]:data,
substr(instructions[0]:data, 0, 1) as first_byte, fee
 from eclipse.core.fact_transactions
where block_timestamp >= current_date - 1
and fee > 50
and instructions[0]:programId = 'ComputeBudget111111111111111111111111111111'
;



WITH tx_info AS (
    SELECT 
        DATE_TRUNC('day', t.block_timestamp) as day_,
        t.tx_id,
        t.fee / pow(10, 9) as fee,
        -- Check if transaction has priority fee flag
        EXISTS (
            SELECT 1
            FROM eclipse.core.fact_events e 
            WHERE e.block_timestamp = t.block_timestamp 
            AND e.tx_id = t.tx_id
            AND e.program_id = 'ComputeBudget111111111111111111111111111111'
            AND substr(utils.udf_base58_to_hex(e.instruction:data), 3, 2) = '03'
        ) as is_priority
    FROM eclipse.core.fact_transactions t
    WHERE t.block_timestamp >= CURRENT_DATE - 7
    AND t.block_timestamp < CURRENT_DATE
),

fee_summary AS (
    SELECT 
        day_,
        SUM(fee) as tx_fees,
        SUM(CASE WHEN NOT is_priority THEN fee ELSE 0 END) as base_fees,
        SUM(CASE WHEN is_priority THEN fee ELSE 0 END) as priority_fees
    FROM tx_info
    GROUP BY 1
),

vote_summary AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) as day_,
        COUNT(DISTINCT tx_id) * 0.00000005 as vote_fees  -- Fixed lamport cost per vote
    FROM eclipse.gov.fact_votes_agg_block -- Changed from fact_votes to fact_votes_agg_block based on schema
    WHERE block_timestamp >= CURRENT_DATE - 7
    AND block_timestamp < CURRENT_DATE
    GROUP BY 1
)

SELECT 
    f.day_,
    f.tx_fees,
    COALESCE(v.vote_fees, 0) as vote_fees,
    f.base_fees,
    f.priority_fees,
    -- Adding some additional context
    f.tx_fees - COALESCE(v.vote_fees, 0) as non_vote_fees,
    (f.priority_fees / NULLIF(f.tx_fees, 0)) * 100 as priority_fee_pct
FROM fee_summary f
LEFT JOIN vote_summary v ON f.day_ = v.day_
ORDER BY day_;