SELECT h.date,
       h.campaign_name,
       i.adset_name,
       h.platform,
       h.ad_source,
       h.impressions_sum,
       IFNULL(h.clicks_sum, 0) AS clicks_sum,
       IFNULL(i.installs, 0) AS installs_sum,
       h.costs_sum
FROM {{ ref("stg_yandex_1_2") }} AS h
LEFT JOIN {{ ref("stg_yandex_1_1" )}} AS i
    ON h.date = i.date AND
       h.campaign_name = i.campaign_name