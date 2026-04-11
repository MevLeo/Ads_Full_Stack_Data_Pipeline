with base as (

    select * from {{ ref('int_ads_unified') }}

),

summary as (

    select
        platform,

        -- volume
        sum(impressions)                                        as total_impressions,
        sum(clicks)                                             as total_clicks,
        sum(conversions)                                        as total_conversions,
        sum(views)                                             as total_views,

        -- cost
        round(sum(spend_usd), 2)                               as total_spend_usd,

        -- derived metrics
        round(sum(clicks) / nullif(sum(impressions), 0), 4)    as ctr,
        round(sum(spend_usd) / nullif(sum(clicks), 0), 2)      as cpc,
        round(sum(spend_usd) / nullif(sum(conversions), 0), 2) as cpa,
        round(avg(roi), 3)                                     as avg_roas,
        round(avg(cpv), 4)                                     as avg_cpv,
        round(avg(view_rate), 3)                               as avg_view_rate,

        -- engagement
        round(avg(engagement_score), 2)                        as avg_engagement_score,

        -- date range
        min(date)                                              as first_date,
        max(date)                                              as last_date,
        count(distinct date)                                   as active_days

    from base
    group by 1

)

select * from summary