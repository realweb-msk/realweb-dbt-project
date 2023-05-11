{{ config(
            materialized='table',
            partition_by = {
                            "field": "date",
                            "data_type": "date",
                            "granularity": "day"
                            }
        )
}}

/*
В huawei и mytarget отсутствует информация о adset_name, хотя в данных af_install она есть.
для vkontakte и yandex в af_install отсутствует информация о adset_name.
назовем неизвестую группу обьявлений 'undefined'.
appsflyer_id - идентификатор, присваиваемый мобильному устройству,
когда пользователь устанавливает приложение.
*/

WITH af_installs AS (
                    SELECT
                        date,
                        campaign_name,
                        CASE adset_name
                            WHEN NULL THEN 'undefined'
                            WHEN '-' THEN 'undefined'
                            WHEN '' THEN 'undefined'
                            WHEN ' ' THEN 'undefined'
                        END AS adset_name,
                        platform,
                        source,
                        COUNT(DISTINCT appsflyer_id) AS installs
                    FROM {{ ref('stg_af_installs') }}
                    WHERE is_realweb
                    AND NOT is_ret_campaign
                    AND source != 'other'
                    AND (platform = 'android' OR platform = 'ios')
                    AND campaign_name IS NOT NULL
                    GROUP BY date, campaign_name, platform, source, adset_name
                    ),

/*
для каждого рекламного кабинета необходим столбец source,
чтобы сопоставить установки из af_installs
*/

facebook AS (
            SELECT
                date,
                campaign_name,
                adset_name,
                platform,
                'facebook' AS source,
                installs,
                clicks,
                costs,
                impressions
            FROM {{ ref('stg_facebook') }}
            WHERE is_realweb
            AND NOT is_ret_campaign
            ),

/*
чтобы при джойне af_installs с таблицами рекламных кабинетов
не получалось большое количество строк с несовпадениями по adset_name,
отсутствие adset_name также заменим на 'undefined'.
*/

google_ads AS (
                SELECT
                    date,
                    campaign_name,
                    adset_name,
                    platform,
                    'google_ads' AS source,
                    installs,
                    clicks,
                    costs,
                    impressions
                FROM {{ ref('stg_google_ads') }}
                WHERE is_realweb
                AND NOT is_ret_campaign
              ),


huawei_ads AS (
                SELECT
                    date,
                    campaign_name,
                    'undefined' AS  adset_name,
                    platform,
                    'huawei' AS source,
                    0 AS installs,
                    clicks,
                    costs,
                    impressions
                FROM {{ ref('stg_huawei_ads') }}
                WHERE is_realweb
                AND NOT is_ret_campaign
               ),

/*
mytarget содержит только значение '-' в столбце adset_name
*/

mytarget AS (
            SELECT
                date,
                campaign_name,
                IF(adset_name = '-', 'undefined', adset_name) AS adset_name,
                platform,
                'mytarget' AS source,
                0 AS installs,
                clicks,
                costs,
                impressions
            FROM {{ ref('stg_mytarget') }}
            WHERE is_realweb
            AND NOT is_ret_campaign
            ),



tiktok AS (
            SELECT
                date,
                campaign_name,
                adset_name,
                platform,
                'tiktok' AS source,
                0 AS installs,
                clicks,
                costs,
                impressions
            FROM {{ ref('stg_tiktok') }}
            WHERE is_realweb
            AND NOT is_ret_campaign
          ),

/*
vkontakte содержит только значение '-' в столбце adset_name
*/

vkontakte AS (
            SELECT
                date,
                campaign_name,
                IF(adset_name = '-', 'undefined', adset_name) AS adset_name,
                platform,
                'vkontakte' AS source,
                0 AS installs,
                clicks,
                costs,
                impressions
            FROM {{ ref('stg_vkontakte') }}
            WHERE is_realweb
            AND NOT is_ret_campaign
            ),

/*
vkontakte содержит значение '--' в столбце adset_name
я считаю это неизвестной группой обьявлений
*/

yandex AS (
            SELECT
                date,
                campaign_name,
                IF(adset_name = '--', 'undefined', adset_name) AS adset_name,
                platform,
                'yandex' AS source,
                0 AS installs,
                clicks,
                costs,
                impressions
            FROM {{ ref('stg_yandex') }}
            WHERE is_realweb
            AND NOT is_ret_campaign
            ),


unioned_sources AS (
                    SELECT *
                    FROM facebook
                    UNION ALL
                    SELECT *
                    FROM google_ads
                    UNION ALL
                    SELECT *
                    FROM huawei_ads
                    UNION ALL
                    SELECT *
                    FROM mytarget
                    UNION ALL
                    SELECT *
                    FROM tiktok
                    UNION ALL
                    SELECT *
                    FROM vkontakte
                    UNION ALL
                    SELECT *
                    FROM yandex
                    ),

/*
если в таблице рекламных кабинетов кол-во скачиваний равно 0,
то можно проверить, если ли эти данные в af_installs,
для этого все 0 заменяю на NULL, чтобы в финальной таблице
сработала проверка IFNULL(IFNULL(filtered_sources.installs, af_installs.installs), 0)
*/

grouped_sources AS (
                    SELECT
                        date,
                        campaign_name,
                        adset_name,
                        platform,
                        source,
                        SUM(installs) AS installs,
                        SUM(costs) AS costs,
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks
                    FROM unioned_sources
                    GROUP BY date, campaign_name, platform, source, adset_name
                    ),

transformed_sources AS (
                    SELECT
                        date,
                        campaign_name,
                        adset_name,
                        platform,
                        source,
                        IF(installs = 0, NULL, installs) AS installs,
                        costs,
                        impressions,
                        clicks
                    FROM grouped_sources
                    WHERE (platform = 'android' OR platform = 'ios')
                    AND campaign_name IS NOT NULL
                    ),

/*
нужно сджойнить таблицы рекламных кабинетов и af_install по столбцам date, campaign_name, platform, source
и adset_name. проблема заключается в том, что информация о adset_name отсуствует af_install
для некоторых источников. в таком случае нужно использовать FULL OUTER JOIN
и заменить на 0 все получившиеся NULL (из-за несоотвествия adset_name в таблицах) в столбцах clicks, costs,
impressions, installs. Это позволит не потерять данные о группах обьявлений, а также не изменит
суммарные кол-ва clicks, costs, impressions, installs в разбивке по источникам, кампаниям, платформам.
в будущем дашборде мы должны учитывать, что для некоторых кампаний информация о installs
не будет разбиваться по группам обьявлений.
 */


final_table AS (
            SELECT
                IFNULL(transformed_sources.date, af_installs.date) AS date,
                IFNULL(transformed_sources.campaign_name, af_installs.campaign_name) AS campaign_name,
                IFNULL(IFNULL(transformed_sources.adset_name, af_installs.adset_name), 'undefined') AS adset_name,
                IFNULL(transformed_sources.platform, af_installs.platform) AS platform,
                IFNULL(transformed_sources.source, af_installs.source) AS source,
                IFNULL(transformed_sources.clicks,0) AS clicks,
                IFNULL(transformed_sources.costs,0) AS costs,
                IFNULL(transformed_sources.impressions,0) AS impressions,
                IFNULL(IFNULL(transformed_sources.installs, af_installs.installs), 0) AS installs
            FROM af_installs
            FULL OUTER JOIN transformed_sources ON af_installs.date = transformed_sources.date
                                            AND af_installs.campaign_name = transformed_sources.campaign_name
                                            AND af_installs.adset_name = transformed_sources.adset_name
                                            AND af_installs.platform = transformed_sources.platform
                                            AND af_installs.source = transformed_sources.source
                ),
/*
в mytarget и facebook присутствуют строки с нулевыми кликами,
показами, стоимостью, скачиваниями (для facebook).
возможно, такая реклама была запланирована на определенный период,
который еще не произошел, и данных нет.
неиформативные строки нужно отфильтровать.
*/

filtered_final_table AS (
                        SELECT
                            date,
                            campaign_name,
                            adset_name,
                            platform,
                            source,
                            clicks,
                            costs,
                            impressions,
                            installs
                        FROM final_table
                        WHERE clicks + costs + impressions + installs > 0
                        )


SELECT
    date,
    campaign_name,
    adset_name,
    platform,
    source,
    clicks,
    costs,
    impressions,
    installs
FROM filtered_final_table