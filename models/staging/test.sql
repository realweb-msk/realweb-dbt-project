
    SELECT SUBSTRING(adset_name, 0, 10) as dd
    FROM {{ref('stg_mytarget')}}
    