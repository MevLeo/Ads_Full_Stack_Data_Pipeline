with base as (

    select * from {{ ref('int_ads_unified') }}

),

performance as (

    select
        platform,
        company,
        customer_segment,
        target_audience,
        campaign_type,

        -- volume
        sum(impressions)                                        as total_impressions,
        sum(clicks)                                             as total_clicks,
        sum(conversions)                                        as total_conversions,

        -- cost
        round(sum(spend_usd), 2)                               as total_spend_usd,

        -- derived metrics
        round(sum(clicks) / nullif(sum(impressions), 0), 4)    as ctr,
        round(sum(spend_usd) / nullif(sum(clicks), 0), 2)      as cpc,
        round(sum(spend_usd) / nullif(sum(conversions), 0), 2) as cpa,
        round(avg(roi), 3)                                     as avg_roas,
        round(avg(cpv), 4)                                     as avg_cpv,
        round(avg(engagement_score), 2)                        as avg_engagement_score

    from base
    group by 1, 2, 3, 4, 5

)

select * from performance