-- Databricks notebook source
select match_id,
       date_format(timestamp(match_datetime), 'yyyy-MM-dd HH:mm') as match_datetime,
       date_format(dt_predict, 'yyyy-MM-dd HH:mm') as dt_predict,
       radiant_score,
       dire_score,
       team_name_dire,
       team_name_radiant,
       proba_radiant_win

from bronze.dota.history_live_predictions

qualify row_number() over (partition by match_id order by dt_predict desc) = 1

order by match_datetime desc, dt_predict desc
