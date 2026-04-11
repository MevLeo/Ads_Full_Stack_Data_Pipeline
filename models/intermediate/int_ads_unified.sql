with google as (

    select
        campaign_id,
        company,
        platform,
        campaign_type,
        target_audience,
        customer_segment,
        location,
        language,
        duration_days,
        date,

        -- volume
        impressions,
        clicks,
        null::float          as views,
        null::float          as view_rate,

        -- cost
        spend_usd,
        cpc,
        null::float          as cpv,

        -- engagement
        engagement_score,
        null::varchar        as video_ad_type,
        null::varchar        as match_type,

        -- conversions
        conversions,
        conversion_rate,
        ctr,

        -- return
        roi,

        -- metadata
        _loaded_at

    from {{ ref('stg_google_ads') }}

),

youtube as (

    select
        campaign_id,
        company,
        platform,
        campaign_type,
        target_audience,
        customer_segment,
        location,
        language,
        duration_days,
        date,

        -- volume
        impressions,
        clicks,
        views,
        view_rate,

        -- cost
        spend_usd,
        cpc,
        cpv,

        -- engagement
        engagement_score,
        video_ad_type,
        null::varchar        as match_type,

        -- conversions
        conversions,
        conversion_rate,
        ctr,

        -- return
        roi,

        -- metadata
        _loaded_at

    from {{ ref('stg_youtube_ads') }}

),

bing as (

    select
        campaign_id,
        company,
        platform,
        campaign_type,
        target_audience,
        customer_segment,
        location,
        language,
        duration_days,
        date,

        -- volume
        impressions,
        clicks,
        null::float          as views,
        null::float          as view_rate,

        -- cost
        spend_usd,
        cpc,
        null::float          as cpv,

        -- engagement
        engagement_score,
        null::varchar        as video_ad_type,
        match_type,

        -- conversions
        conversions,
        conversion_rate,
        ctr,

        -- return
        roi,

        -- metadata
        _loaded_at

    from {{ ref('stg_bing_ads') }}

),

unified as (

    select * from google
    union all
    select * from youtube
    union all
    select * from bing

)

select * from unified