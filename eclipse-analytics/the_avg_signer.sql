with signer_tx_history AS (
    select signers[0] as signer, 
count(tx_id) as txs, -- already 1 tx per row 
count(distinct date_trunc('day', block_timestamp)) as days_active,
DIV0(txs, days_active) as txs_per_active_day,
min(block_timestamp) as first_tx_date,
max(block_timestamp) as last_tx_date,
datediff('day', first_tx_date, current_date) as days_since_first_tx
from eclipse.core.fact_transactions
  where signers[0] != 'G5FM3UKwcBJ47PwLWLLY1RQpqNtTMgnqnd6nZGcJqaBp' -- hyperlane signer
group by signer
),

signer_program_history AS (
    select signers[0] as signer, 
    count(distinct program_id) as programs_used
    from eclipse.core.fact_events
     where program_id not in (
    'BPFLoaderUpgradeab1e11111111111111111111111'
    , 'ComputeBudget111111111111111111111111111111'
    , '11111111111111111111111111111111'
    , 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
    , 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
    , 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
    , 'FUCHhfHbuxXBWiRBfVdhmiog84sUJw11aAq3ibAUGL6e'  -- a lot of txs but only 2 signers
    , '5hEa5j38yNJRM9vQA44Q6gXVj4Db8y3mWxkDtQeofKKs' -- very probable a airdrop botting farmer
      )
  and signers[0] != 'G5FM3UKwcBJ47PwLWLLY1RQpqNtTMgnqnd6nZGcJqaBp' -- hyperlane signer
    group by signer
)

 SELECT 
        AVG(txs) as avg_txs,
        median(txs) as median_txs,
        AVG(txs_per_active_day) as avg_txs_per_active_day,
        median(txs_per_active_day) as median_txs_per_active_day,
        (select AVG(programs_used) from signer_program_history) as avg_programs_used,
        AVG(days_active) as avg_days_active,
        AVG(days_since_first_tx) as avg_days_since_first_tx
    FROM signer_tx_history;