-- One row per keyword x geo x run_date.
-- Enriched with 7-day average interest from interest_over_time.

with trending as (
    select * from {{ ref('stg_trending_searches') }}
),

interest as (
    select
        geo,
        keyword,
        trend_date,
        avg(interest_value) as avg_interest
    from {{ ref('stg_interest_over_time') }}
    group by geo, keyword, trend_date
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['t.run_ts', 't.geo', 't.keyword']) }}
                                                    as trend_id,
        t.run_ts,
        t.run_date,
        t.geo,
        t.keyword,
        t.rank,
        coalesce(i.avg_interest, 0)                 as avg_interest_score,
        t.ingested_at
    from trending t
    left join interest i
        on  t.geo       = i.geo
        and t.keyword   = i.keyword
        and t.run_date  = i.trend_date
)

select * from final