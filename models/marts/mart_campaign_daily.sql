with base as (

    select * from {{ ref('int_ads_unified') }}

),

daily as (

    select
        date,
        platform,
        company,
        campaign_type,
        location,

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

        -- ROAS: using roi as proxy for revenue multiple
        round(avg(roi), 3)                                     as avg_roas,

        -- youtube specific
        round(sum(views), 0)                                   as total_views,
        round(avg(view_rate), 3)                               as avg_view_rate,
        round(avg(cpv), 4)                                     as avg_cpv

    from base
    group by 1, 2, 3, 4, 5

)

select * from daily