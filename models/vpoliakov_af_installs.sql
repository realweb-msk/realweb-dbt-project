{% if target.name == 'prod' %}

{{
  config(
    materialized='table',
    partition_by = {
            "field": "date",
            "data_type": "date",
            "granularity": "day"
            },
  )
}}

{% endif %}

--агрегируем и фильтруем данные AF
WITH af_installs_agg AS
(
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    COUNT (DISTINCT appsflyer_id) AS af_installs,
FROM
    {{ ref('stg_af_installs') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
    AND source != 'other'
GROUP BY
    1,2,3,4,5
),

--выделяем данные AF по huawei и mytarget в отдельные временные таблицы:
--для них есть группы в AF, но нет групп в кабинетах, при этом есть кампании, попавшие больше чем в одну группу
--чтобы избежать дублирования кликов и показов, агрегируем данные AF по названию кампании и джойним с данным кабинетов

--выделяем mytarget из AF
af_mt AS
(
SELECT 
    date,
    campaign_name,
    platform,
    SUM(af_installs) AS installs,
FROM
    af_installs_agg
WHERE 
    source IN ('mytarget')
GROUP BY 
    1, 2, 3
),

--фильтруем кабинет
cab_mt AS
(
SELECT 
    date,
    campaign_name,
    platform,
    clicks,
    costs,
    impressions,
FROM {{ ref('stg_mytarget') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

--объединяем данные mytarget с кабинетом
mt_installs AS
(
SELECT 
    date,
    campaign_name,
    CAST (NULL AS STRING) AS adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    c.clicks,
    c.costs,
    agg.installs,
    c.impressions,
FROM 
    af_mt AS agg
FULL JOIN 
    cab_mt AS c USING (date, campaign_name, platform)
),

--собираем данные huawei
af_hw AS
(
SELECT 
    date,
    campaign_name,
    platform,
    SUM(af_installs) AS installs,
FROM
    af_installs_agg
WHERE 
    source IN ('huawei')
GROUP BY 
    1, 2, 3
),

--фильтруем данные из кабинетов
cab_hw AS
(
SELECT 
    date,
    campaign_name,
    platform,
    clicks,
    costs,
    impressions,
FROM {{ ref('stg_huawei_ads') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign

),
--соединяем данные AF и кабинетов для huawei
hw_installs AS
(
SELECT 
    date,
    campaign_name,
    CAST (NULL AS STRING) adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    cab.clicks,
    cab.costs,
    hw.installs,
    cab.impressions,
FROM 
    af_hw AS hw
LEFT JOIN 
    cab_hw AS cab USING(date, campaign_name, platform)
),

--логика google и Fb: сначала выбираем из AF те сочетания даты-кампании-группы-платформы, которых нет в кабинетах, 
--добавляем к ним нужные столбцы, заполненные NULL (если нет в кабинетах, данных о кликах и т.п. тоже нет),
--делаем UNION - так мы сохраним установки из AF для тех кампаний/групп, которых нет в кабинете

--выбираем из Google кабинета нужные строки
cab_gl AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    clicks,
    costs,
    installs,
    impressions,
FROM
    {{ ref('stg_google_ads') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

--создаем список кампаний и групп, которые есть в кабинете
gl_list AS
(
SELECT
    CONCAT(CAST(date AS STRING), campaign_name, IFNULL(adset_name, 'no_adset')) AS gl_campaign_list --заполнение null, чтобы concat не возвращал NULL
FROM
    cab_gl
),

--выделяем google из AF - то, чего нет в кабинете
af_gl AS
(
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    CAST(NULL AS INT64) AS clicks,
    CAST(NULL AS INT64) AS costs,
    af_installs AS installs,
    CAST(NULL AS INT64) AS impressions,
FROM
    af_installs_agg
WHERE 
    source = 'google_ads'
    AND CONCAT(CAST (date AS STRING), campaign_name, IFNULL(adset_name, 'no_adset')) NOT IN (SELECT gl_campaign_list FROM gl_list)
),

--собираем таблички gl
gl_installs AS
(
SELECT *
FROM 
    cab_gl
UNION ALL
SELECT *
FROM   
    af_gl
),

--все то же самое для fb
--выбираем из fb кабинета нужные строки
cab_fb AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    clicks,
    costs,
    installs,
    impressions,
FROM
    {{ ref('stg_facebook') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

--создаем список кампаний и групп, которые есть в кабинете
fb_list AS
(
SELECT
    CONCAT(CAST(date AS STRING), campaign_name, IFNULL(adset_name, 'no_adset')) AS fb_campaign_list 
FROM
    cab_fb
),

--выделяем fb 
af_fb AS
(
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    CAST(NULL AS INT64) AS clicks,
    CAST(NULL AS INT64) AS costs,
    af_installs AS installs,
    CAST(NULL AS INT64) AS impressions,
FROM
    af_installs_agg
WHERE 
    source = 'facebook'
    AND CONCAT(CAST (date AS STRING), campaign_name, IFNULL(adset_name, 'no_adset')) NOT IN (SELECT fb_campaign_list FROM fb_list)
),

--собираем таблички fb
fb_installs AS
(
SELECT *
FROM 
    cab_fb
UNION ALL
SELECT *
FROM   
    af_fb
),

--выделяем vk
af_vk AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE
    source IN ('vkontakte')
),

--фильтруем кабинет
cab_vk AS
(
SELECT
    date,
    campaign_name,
    platform,
    clicks,
    costs,
    impressions,
FROM
    {{ ref('stg_vkontakte') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

--собираем данные по vk
vk_installs AS
(
SELECT
    date,
    campaign_name,
    CAST (NULL AS STRING) adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    co.clicks,
    co.costs,
    af.installs,
    co.impressions,
FROM 
    af_vk AS af
FULL JOIN
    cab_vk AS co USING(date, campaign_name, platform)
),

--собираем данные по ya
af_ya AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE 
    source IN ('yandex')
),

--в кабинете yandex нет установок, а в AF - данных по adset yandex
--суммируем клики и т.д. по названию кампании, чтобы не задублировать число установок из-за разных групп
cab_ya AS 
(
SELECT 
    date,
    campaign_name,
    platform,
    SUM(clicks) AS clicks,
    SUM(costs) AS costs,
    SUM(impressions) AS impressions,
FROM 
    {{ ref('stg_yandex') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
GROUP BY
    1, 2, 3
),

ya_installs AS
(
SELECT
    date,
    campaign_name,
    CAST(NULL AS STRING) AS adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    aya.clicks,
    aya.costs,
    ya.installs,
    aya.impressions,
FROM 
    af_ya AS ya
FULL JOIN
    cab_ya AS aya USING(date, campaign_name, platform)
),

--добираем данные AF по tiktok
af_tk AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    af_installs AS installs,
FROM 
    af_installs_agg
WHERE
    source IN ('tiktok')
),

--фильтруем кабинет
cab_tk AS
(
SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    clicks,
    costs,
    impressions,
FROM
    {{ ref('stg_tiktok') }}
WHERE
    is_realweb
    AND NOT is_ret_campaign
),

tk_installs AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    {{ install_source('campaign_name') }} AS source,
    tk.clicks,
    tk.costs,
    a.installs,
    tk.impressions,
FROM 
    af_tk AS a
FULL JOIN
    cab_tk AS tk USING (date, campaign_name, adset_name, platform)
),

final AS
(
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    vk_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    gl_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    fb_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    hw_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    mt_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    tk_installs
UNION ALL
SELECT 
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    installs,
    impressions,
FROM 
    ya_installs
)

SELECT *
FROM final