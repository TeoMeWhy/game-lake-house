with tb_game_stats as (

  select account_id,
         count(distinct match_id) as qtMatches,
         min(datediff('{date}', dt_match )) as qtDaysLastMatch,
         count(distinct match_id) / count(distinct month(dt_match)) as qtMatchMonth,
         count(distinct match_id) / 6 as qtMatchMonth6,
         avg(win) as avgWinRate,
         avg(kills_per_min) vlAvgKillMinute,
         avg(assists) as vlAvgAssists,
         avg(camps_stacked) as vlAvgCampsStacked,
         avg(creeps_stacked) as vlAvgCreepsStacked,
         avg(deaths) as vlAvgDeaths,
         avg(denies) as vlAvgDenies,
         avg(firstblood_claimed) as vlAvgFirstblood,
         avg(gold_per_min) as vlAvgGoldMinute,
         avg(kills) as vlAvgKills,
         avg(rune_pickups) as vlAvgRune,
         avg(xp_per_min) as vlAvgXpMinute,
         avg(kda) as vlAvgKDA,
         avg(neutral_kills) as vlAvgNeutralKills ,
         avg(observer_kills) as vlAvgObserverKills,
         avg(sentry_kills) as vlAvgSentryKills,
         sum(case when lane_role = 1 then 1 else 0 end) / count(distinct match_id) as pctLaneRole1,
         sum(case when lane_role = 2 then 1 else 0 end) / count(distinct match_id) as pctLaneRole2,
         sum(case when lane_role = 3 then 1 else 0 end) / count(distinct match_id) as pctLaneRole3,
         sum(case when lane_role = 4 then 1 else 0 end) / count(distinct match_id) as pctLaneRole4

  from bronze_gamelakehouse.dota_match_players
  where dt_match >= add_months('{date}', -6)
  and dt_match < '{date}'
  group by account_id

),

tb_lifetime as (

  select account_id,
         max(datediff('{date}', dt_match)) as qtDaysFirstMatch
  from bronze_gamelakehouse.dota_match_players
  group by 1

)

select
      '{date}' as dtReference,
      t1.account_id,
      coalesce(t1.qtMatches, 0) as qtMatches,
      coalesce(t1.qtMatchMonth, 0) as qtMatchMonth,
      coalesce(t1.qtMatchMonth6, 0) as qtMatchMonth6,
      coalesce(t1.qtDaysLastMatch, 0) as qtDaysLastMatch,
      coalesce(t2.qtDaysFirstMatch, 0) as qtDaysFirstMatch,
      coalesce(t1.avgWinRate, 0) as avgWinRate,
      coalesce(t1.vlAvgKillMinute, 0) as vlAvgKillMinute,
      coalesce(t1.vlAvgAssists, 0) as vlAvgAssists,
      coalesce(t1.vlAvgCampsStacked, 0) as vlAvgCampsStacked,
      coalesce(t1.vlAvgCreepsStacked, 0) as vlAvgCreepsStacked,
      coalesce(t1.vlAvgDeaths, 0) as vlAvgDeaths,
      coalesce(t1.vlAvgDenies, 0) as vlAvgDenies,
      coalesce(t1.vlAvgFirstblood, 0) as vlAvgFirstblood,
      coalesce(t1.vlAvgGoldMinute, 0) as vlAvgGoldMinute,
      coalesce(t1.vlAvgKills, 0) as vlAvgKills,
      coalesce(t1.vlAvgRune, 0) as vlAvgRune,
      coalesce(t1.vlAvgXpMinute, 0) as vlAvgXpMinute,
      coalesce(t1.vlAvgKDA, 0) as vlAvgKDA,
      coalesce(t1.vlAvgNeutralKills, 0) as vlAvgNeutralKills,
      coalesce(t1.vlAvgObserverKills, 0) as vlAvgObserverKills,
      coalesce(t1.vlAvgSentryKills, 0) as vlAvgSentryKills,
      coalesce(t1.pctLaneRole1, 0) as pctLaneRole1,
      coalesce(t1.pctLaneRole2, 0) as pctLaneRole2,
      coalesce(t1.pctLaneRole3, 0) as pctLaneRole3,
      coalesce(t1.pctLaneRole4, 0) as pctLaneRole4

from tb_game_stats as t1

left join tb_lifetime as t2
on t1.account_id = t2.account_id