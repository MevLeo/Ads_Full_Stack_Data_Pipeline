with source as (

    select * from {{ source('bronze', 'youtube_ads_raw') }}

),

cleaned as (

    select
        -- identifiers
        campaign_id,
        company,
        'youtube_ads'                                   as platform,

        -- campaign attributes
        campaign_type,
        target_audience,
        customer_segment,
        location,
        language,
        cast(replace(duration, ' days', '') as integer) as duration_days,

        -- time
        cast(date as date)                              as date,

        -- volume metrics
        impressions,
        clicks,
        views,
        view_rate,

        -- cost metrics
        spend_usd,
        cpv,
        round(spend_usd / nullif(clicks, 0), 4)        as cpc,

        -- engagement
        engagement_score,
        video_ad_type,

        -- conversion metrics
        conversions,
        conversion_rate,
        round(clicks / nullif(impressions, 0), 4)      as ctr,

        -- return
        roi,

        -- metadata
        _loaded_at

    from source

    where campaign_type not in ('Email', 'Influencer')

)

select * from cleaned