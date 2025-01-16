WITH 
-- Arbitrum metrics
top_10k_arbitrum AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.arbitrum
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
arbitrum_metrics AS (
    SELECT 
        'arbitrum' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM arbitrum.core.fact_transactions ft 
    INNER JOIN top_10k_arbitrum tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
arbitrum_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM arbitrum.core.ez_token_transfers ett
        INNER JOIN top_10k_arbitrum tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM arbitrum.core.ez_native_transfers ent
        INNER JOIN top_10k_arbitrum tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
arbitrum_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM arbitrum_metrics m
    INNER JOIN top_10k_arbitrum tb 
        ON m.from_address = tb.user_address
    INNER JOIN arbitrum_volumes v 
        ON m.from_address = v.origin_from_address
),

-- Base metrics
top_10k_base AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.base
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
base_metrics AS (
    SELECT 
        'base' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM base.core.fact_transactions ft 
    INNER JOIN top_10k_base tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
base_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM base.core.ez_token_transfers ett
        INNER JOIN top_10k_base tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM base.core.ez_native_transfers ent
        INNER JOIN top_10k_base tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
base_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM base_metrics m
    INNER JOIN top_10k_base tb 
        ON m.from_address = tb.user_address
    INNER JOIN base_volumes v 
        ON m.from_address = v.origin_from_address
),

-- BSC metrics
top_10k_bsc AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.bsc
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
bsc_metrics AS (
    SELECT 
        'bsc' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM bsc.core.fact_transactions ft 
    INNER JOIN top_10k_bsc tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
bsc_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM bsc.core.ez_token_transfers ett
        INNER JOIN top_10k_bsc tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM bsc.core.ez_native_transfers ent
        INNER JOIN top_10k_bsc tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
bsc_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM bsc_metrics m
    INNER JOIN top_10k_bsc tb 
        ON m.from_address = tb.user_address
    INNER JOIN bsc_volumes v 
        ON m.from_address = v.origin_from_address
),

-- Ethereum metrics
top_10k_ethereum AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.ethereum
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
ethereum_metrics AS (
    SELECT 
        'ethereum' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM ethereum.core.fact_transactions ft 
    INNER JOIN top_10k_ethereum tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
ethereum_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM ethereum.core.ez_token_transfers ett
        INNER JOIN top_10k_ethereum tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM ethereum.core.ez_native_transfers ent
        INNER JOIN top_10k_ethereum tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
ethereum_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM ethereum_metrics m
    INNER JOIN top_10k_ethereum tb 
        ON m.from_address = tb.user_address
    INNER JOIN ethereum_volumes v 
        ON m.from_address = v.origin_from_address
),

-- Optimism metrics
top_10k_optimism AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.optimism
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
optimism_metrics AS (
    SELECT 
        'optimism' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM optimism.core.fact_transactions ft 
    INNER JOIN top_10k_optimism tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
optimism_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM optimism.core.ez_token_transfers ett
        INNER JOIN top_10k_optimism tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM optimism.core.ez_native_transfers ent
        INNER JOIN top_10k_optimism tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
optimism_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM optimism_metrics m
    INNER JOIN top_10k_optimism tb 
        ON m.from_address = tb.user_address
    INNER JOIN optimism_volumes v 
        ON m.from_address = v.origin_from_address
),

-- Polygon metrics
top_10k_polygon AS (
    SELECT blockchain, user_address, total_score, score_date
    FROM datascience.onchain_scores.polygon
    WHERE score_date = '2024-12-31'
    ORDER BY total_score DESC, user_address DESC
    LIMIT 10000
),
polygon_metrics AS (
    SELECT 
        'polygon' as blockchain,
        from_address,
        MAX(nonce) as tx_count_lifetime,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) as distinct_months_active
    FROM polygon.core.fact_transactions ft 
    INNER JOIN top_10k_polygon tb 
        ON ft.from_address = tb.user_address
    WHERE block_timestamp >= DATEADD('month', -48, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
polygon_volumes AS (
    SELECT 
        origin_from_address,
        SUM(COALESCE(amount_usd, 0)) as total_usd_volume
    FROM (
        SELECT ett.origin_from_address, ett.amount_usd 
        FROM polygon.core.ez_token_transfers ett
        INNER JOIN top_10k_polygon tb 
            ON ett.origin_from_address = tb.user_address
        UNION ALL
        SELECT ent.origin_from_address, ent.amount_usd
        FROM polygon.core.ez_native_transfers ent
        INNER JOIN top_10k_polygon tb 
            ON ent.origin_from_address = tb.user_address
    )
    GROUP BY 1
),
polygon_results AS (
    SELECT 
        m.blockchain,
        m.from_address as address,
        tb.total_score,
        tb.score_date,
        m.tx_count_lifetime,
        v.total_usd_volume as lifetime_usd_volume,
        m.distinct_months_active
    FROM polygon_metrics m
    INNER JOIN top_10k_polygon tb 
        ON m.from_address = tb.user_address
    INNER JOIN polygon_volumes v 
        ON m.from_address = v.origin_from_address
)

-- Final combination of all chains
SELECT * FROM arbitrum_results
UNION ALL
SELECT * FROM base_results
UNION ALL
SELECT * FROM bsc_results
UNION ALL
SELECT * FROM ethereum_results
UNION ALL
SELECT * FROM optimism_results
UNION ALL
SELECT * FROM polygon_results
ORDER BY blockchain, total_score DESC