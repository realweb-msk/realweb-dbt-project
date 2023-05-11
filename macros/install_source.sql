{% macro install_source(campaign_name) %}
    CASE REGEXP_EXTRACT(TRIM(LOWER({{campaign_name}})), r'realweb_([a-z]+)_')
        WHEN 'fb' THEN 'facebook'
        WHEN 'hw' THEN 'huawei'
        WHEN 'mt' THEN 'mytarget'
        WHEN 'android' THEN 'mytarget' 
        WHEN 'tiktok' THEN 'tiktok'
        WHEN 'uac' THEN 'google_ads'
        WHEN 'vk' THEN 'vkontakte'
        WHEN 'ya' THEN 'yandex'
        ELSE 'other' END
{% endmacro %}

-- добавила строку WHEN 'android' THEN 'mytarget', т.к в кабинете есть названия кампаний в таком виде – realweb_android_mytarget_msk_spb_new_dec1220_darkstore_install
-- сначала хотела условие поменять на REGEXP_EXTRACT(TRIM(LOWER({{campaign_name}})), r'(realweb_([a-z]+)_|realweb_android_([a-z]+)_'), оказалось так нельзя(
    