-- Daily interest score per keyword x geo, ready for time-series charts.

with base as (
    select
        geo,
        keyword,
        trend_date,
        avg(interest_value)     as avg_interest,
        max(interest_value)     as peak_interest,
        count(*)                as data_points
    from {{ ref('stg_interest_over_time') }}
    group by geo, keyword, trend_date
),

with_geo as (
    select
        b.*,
        g.country_name,
        g.region
    from base b
    left join {{ ref('dim_geo') }} g on b.geo = g.geo_code
)

select * from with_geo