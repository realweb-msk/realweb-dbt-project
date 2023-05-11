SELECT  date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(*) AS installs
FROM {{ ref("stg_af_installs") }}
WHERE source = 'tiktok' 
GROUP BY date,
        campaign_name,
        adset_name,
        platform,
        source