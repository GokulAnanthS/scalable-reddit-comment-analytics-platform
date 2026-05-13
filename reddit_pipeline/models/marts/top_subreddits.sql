SELECT
    subreddit,
    total_score,
    total_posts,
    avg_score,

    ROW_NUMBER() OVER (
        ORDER BY total_score DESC
    ) AS subreddit_rank

FROM GOLD.GOLD_SUBREDDIT_METRICS

QUALIFY subreddit_rank <= 10