{% if target.name == 'prod' %}
    {{ config(
        materialized = 'table',
        partition_by ={ "field": "date",
        "data_type": "date",
        "granularity": "day" }
    ) }}
{% endif %}

WITH af_installs AS (
-- создание таблицы с данными по установкам приложения с разбивкой по дате, кампании, группе объявлений, платформе и источнику
-- в таблице присутствует фильтр по заданным условиям
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        COUNT(DISTINCT(appsflyer_id)) AS installs
    FROM
        {{ ref('stg_af_installs') }}
    WHERE
        is_realweb
        AND source != 'other'
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform,
        source
),
-- создание таблицы с данными по установкам, кликам, показам и расходам для каждого источника с разбивкой по дате, кампании, группе объявлений, платформе и источнику
-- в таблицах присутствует фильтр по заданным условиям
facebook AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'facebook' AS source, --поле source в исходных данных отсутствует
        SUM(installs) AS installs,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_facebook') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
google_ads AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'google_ads' AS source, --поле source в исходных данных отсутствует
        SUM(installs) AS installs,
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_google_ads') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
huawei AS (
    SELECT
        date,
        campaign_name,
        'n/a' AS adset_name, --поле adset_name в исходных данных отсутствует
        platform,
        'huawei_ads' AS source, --поле source в исходных данных отсутствует
        NULL AS installs, --поле installs в исходных данных отсутствует
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_huawei_ads') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
mytarget AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'mytarget' AS source, --поле source в исходных данных отсутствует
        NULL AS installs, --поле installs в исходных данных отсутствует
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_mytarget') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
tiktok AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'tiktok' AS source, --поле source в исходных данных отсутствует
        NULL AS installs, --поле installs в исходных данных отсутствует
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_tiktok') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
vkontakte AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'vkontakte' AS source, --поле source в исходных данных отсутствует
        NULL AS installs, --поле installs в исходных данных отсутствует
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_vkontakte') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
yandex AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        'yandex' AS source, --поле source в исходных данных отсутствует
        NULL AS installs, --поле installs в исходных данных отсутствует
        SUM(clicks) AS clicks,
        SUM(impressions) AS impressions,
        SUM(costs) AS costs
    FROM
        {{ ref('stg_yandex') }}
    WHERE
        is_realweb
        AND NOT is_ret_campaign
    GROUP BY
        date,
        campaign_name,
        adset_name,
        platform
),
-- создание таблицы-объединения данных по установкам, кликам, показам и расходам из таблиц каждого источника
union_all AS (
    SELECT
        *
    FROM
        facebook
    UNION ALL
    SELECT
        *
    FROM
        google_ads
    UNION ALL
    SELECT
        *
    FROM
        huawei
    UNION ALL
    SELECT
        *
    FROM
        mytarget
    UNION ALL
    SELECT
        *
    FROM
        tiktok
    UNION ALL
    SELECT
        *
    FROM
        vkontakte
    UNION ALL
    SELECT
        *
    FROM
        yandex
),
--результирующая таблица, объединяющая данные из всех источников
final AS (
    SELECT
        date,
        campaign_name,
        adset_name,
        platform,
        source,
        -- количество кликов объединенных данных или 0, если такого поля нет
        IFNULL(
            union_all.clicks, 
            0
        ) AS clicks,
        -- количество показов объединенных данных или 0, если такого поля нет
        IFNULL(
            union_all.impressions, 
            0
        ) AS impressions,
        -- количество установок объединенных данных или 0, если такого поля нет
        -- количество установок из таблицы af_installs или 0, если такого поля нет в обеих таблицах
        IFNULL(
            IFNULL(
                union_all.installs, 
                af_installs.installs 
            ),
            0
        ) AS installs,
        -- расходы объединенных данных или 0, если такого поля нет
        IFNULL(
            union_all.costs, 
            0.0
        ) AS costs
    FROM
        union_all
        -- FULL OUTER JOIN объединяет данные таблицы union_all и af_installs
        FULL OUTER JOIN af_installs USING ( 
            date,
            campaign_name,
            adset_name,
            platform,
            source
        )
)
-- вывод результирующей таблицы
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    impressions,
    installs,
    costs
FROM
    final 
