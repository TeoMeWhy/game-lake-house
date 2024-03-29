-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.abt_match_predict;
CREATE TABLE silver.dota.abt_match_predict

WITH tb_match_player AS (

  SELECT match_id,
         DATE(t1.dt_match) AS dt_match,
         account_id,
         isRadiant AS is_radiant,
         radiant_win,
         hero_id

  FROM bronze.dota.match_players AS t1
  WHERE dt_match >= '2020-01-01'

),

tb_match AS (

  SELECT match_id,
         MAX(radiant_win) as radiant_win
  FROM tb_match_player
  GROUP BY match_id

),

tb_heros AS (

  SELECT

  t1.match_id,
  t1.dt_match,
  t1.account_id,
  t1.is_radiant,
  t1.hero_id,
  t2.avgWinHero,
  t2.avgKdaHero,
  t2.avgDeathsHero,
  t2.avgXpPerMinHero,
  t2.avgGoldPerMinHero,

  ROW_NUMBER() OVER (PARTITION BY match_id, is_radiant ORDER BY COALESCE(t2.avgWinHero, 0) DESC) AS rnWinHero,
  ROW_NUMBER() OVER (PARTITION BY match_id, is_radiant ORDER BY COALESCE(t2.avgKdaHero, 0) DESC) AS rnKdaHero,
  ROW_NUMBER() OVER (PARTITION BY match_id, is_radiant ORDER BY COALESCE(t2.avgDeathsHero, 0) DESC) AS rnDeathsHero,
  ROW_NUMBER() OVER (PARTITION BY match_id, is_radiant ORDER BY COALESCE(t2.avgXpPerMinHero, 0) DESC) AS rnXpPerMinHero,
  ROW_NUMBER() OVER (PARTITION BY match_id, is_radiant ORDER BY COALESCE(t2.avgGoldPerMinHero, 0) DESC) AS rnGoldPerMinHero

  FROM tb_match_player AS t1

  INNER JOIN silver.dota.fs_player_hero_summary AS t2
  ON date(t1.dt_match) = t2.dtReference
  AND t2.account_id = t1.account_id
  AND t2.hero_id = t1.hero_id

  ORDER BY match_id, is_radiant

),

tb_match_hero_summary AS (

SELECT match_id,
      sum(case when rnWinHero = 1 and is_radiant = TRUE then avgWinHero end) as avgWinHeroRandiant1,
      sum(case when rnWinHero = 2 and is_radiant = TRUE then avgWinHero end) as avgWinHeroRandiant2,
      sum(case when rnWinHero = 3 and is_radiant = TRUE then avgWinHero end) as avgWinHeroRandiant3,
      sum(case when rnWinHero = 4 and is_radiant = TRUE then avgWinHero end) as avgWinHeroRandiant4,
      sum(case when rnWinHero = 5 and is_radiant = TRUE then avgWinHero end) as avgWinHeroRandiant5,
      sum(case when rnKdaHero = 1 and is_radiant = TRUE then avgKdaHero end) as avgKdaHeroRandiant1,
      sum(case when rnKdaHero = 2 and is_radiant = TRUE then avgKdaHero end) as avgKdaHeroRandiant2,
      sum(case when rnKdaHero = 3 and is_radiant = TRUE then avgKdaHero end) as avgKdaHeroRandiant3,
      sum(case when rnKdaHero = 4 and is_radiant = TRUE then avgKdaHero end) as avgKdaHeroRandiant4,
      sum(case when rnKdaHero = 5 and is_radiant = TRUE then avgKdaHero end) as avgKdaHeroRandiant5,
      sum(case when rnDeathsHero = 1 and is_radiant = TRUE then avgDeathsHero end) as avgDeathsHeroRandiant1,
      sum(case when rnDeathsHero = 2 and is_radiant = TRUE then avgDeathsHero end) as avgDeathsHeroRandiant2,
      sum(case when rnDeathsHero = 3 and is_radiant = TRUE then avgDeathsHero end) as avgDeathsHeroRandiant3,
      sum(case when rnDeathsHero = 4 and is_radiant = TRUE then avgDeathsHero end) as avgDeathsHeroRandiant4,
      sum(case when rnDeathsHero = 5 and is_radiant = TRUE then avgDeathsHero end) as avgDeathsHeroRandiant5,
      sum(case when rnXpPerMinHero = 1 and is_radiant = TRUE then avgXpPerMinHero end) as avgXpPerMinHeroRandiant1,
      sum(case when rnXpPerMinHero = 2 and is_radiant = TRUE then avgXpPerMinHero end) as avgXpPerMinHeroRandiant2,
      sum(case when rnXpPerMinHero = 3 and is_radiant = TRUE then avgXpPerMinHero end) as avgXpPerMinHeroRandiant3,
      sum(case when rnXpPerMinHero = 4 and is_radiant = TRUE then avgXpPerMinHero end) as avgXpPerMinHeroRandiant4,
      sum(case when rnXpPerMinHero = 5 and is_radiant = TRUE then avgXpPerMinHero end) as avgXpPerMinHeroRandiant5,
      sum(case when rnGoldPerMinHero = 1 and is_radiant = TRUE then avgGoldPerMinHero end) as avgGoldPerMinHeroRandiant1,
      sum(case when rnGoldPerMinHero = 2 and is_radiant = TRUE then avgGoldPerMinHero end) as avgGoldPerMinHeroRandiant2,
      sum(case when rnGoldPerMinHero = 3 and is_radiant = TRUE then avgGoldPerMinHero end) as avgGoldPerMinHeroRandiant3,
      sum(case when rnGoldPerMinHero = 4 and is_radiant = TRUE then avgGoldPerMinHero end) as avgGoldPerMinHeroRandiant4,
      sum(case when rnGoldPerMinHero = 5 and is_radiant = TRUE then avgGoldPerMinHero end) as avgGoldPerMinHeroRandiant5,

      sum(case when rnWinHero = 1 and is_radiant = FALSE then avgWinHero end) as avgWinHeroDire1,
      sum(case when rnWinHero = 2 and is_radiant = FALSE then avgWinHero end) as avgWinHeroDire2,
      sum(case when rnWinHero = 3 and is_radiant = FALSE then avgWinHero end) as avgWinHeroDire3,
      sum(case when rnWinHero = 4 and is_radiant = FALSE then avgWinHero end) as avgWinHeroDire4,
      sum(case when rnWinHero = 5 and is_radiant = FALSE then avgWinHero end) as avgWinHeroDire5,
      sum(case when rnKdaHero = 1 and is_radiant = FALSE then avgKdaHero end) as avgKdaHeroDire1,
      sum(case when rnKdaHero = 2 and is_radiant = FALSE then avgKdaHero end) as avgKdaHeroDire2,
      sum(case when rnKdaHero = 3 and is_radiant = FALSE then avgKdaHero end) as avgKdaHeroDire3,
      sum(case when rnKdaHero = 4 and is_radiant = FALSE then avgKdaHero end) as avgKdaHeroDire4,
      sum(case when rnKdaHero = 5 and is_radiant = FALSE then avgKdaHero end) as avgKdaHeroDire5,
      sum(case when rnDeathsHero = 1 and is_radiant = FALSE then avgDeathsHero end) as avgDeathsHeroDire1,
      sum(case when rnDeathsHero = 2 and is_radiant = FALSE then avgDeathsHero end) as avgDeathsHeroDire2,
      sum(case when rnDeathsHero = 3 and is_radiant = FALSE then avgDeathsHero end) as avgDeathsHeroDire3,
      sum(case when rnDeathsHero = 4 and is_radiant = FALSE then avgDeathsHero end) as avgDeathsHeroDire4,
      sum(case when rnDeathsHero = 5 and is_radiant = FALSE then avgDeathsHero end) as avgDeathsHeroDire5,
      sum(case when rnXpPerMinHero = 1 and is_radiant = FALSE then avgXpPerMinHero end) as avgXpPerMinHeroDire1,
      sum(case when rnXpPerMinHero = 2 and is_radiant = FALSE then avgXpPerMinHero end) as avgXpPerMinHeroDire2,
      sum(case when rnXpPerMinHero = 3 and is_radiant = FALSE then avgXpPerMinHero end) as avgXpPerMinHeroDire3,
      sum(case when rnXpPerMinHero = 4 and is_radiant = FALSE then avgXpPerMinHero end) as avgXpPerMinHeroDire4,
      sum(case when rnXpPerMinHero = 5 and is_radiant = FALSE then avgXpPerMinHero end) as avgXpPerMinHeroDire5,
      sum(case when rnGoldPerMinHero = 1 and is_radiant = FALSE then avgGoldPerMinHero end) as avgGoldPerMinHeroDire1,
      sum(case when rnGoldPerMinHero = 2 and is_radiant = FALSE then avgGoldPerMinHero end) as avgGoldPerMinHeroDire2,
      sum(case when rnGoldPerMinHero = 3 and is_radiant = FALSE then avgGoldPerMinHero end) as avgGoldPerMinHeroDire3,
      sum(case when rnGoldPerMinHero = 4 and is_radiant = FALSE then avgGoldPerMinHero end) as avgGoldPerMinHeroDire4,
      sum(case when rnGoldPerMinHero = 5 and is_radiant = FALSE then avgGoldPerMinHero end) as avgGoldPerMinHeroDire5

FROM tb_heros

GROUP BY match_id

),

tb_summary_player as (

  SELECT t1.match_id,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.qtRecency END) AS qtRecencyRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.qtMatches END) AS qtMatchesRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgAssists END) AS avgAssistsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgCampsStacked END) AS avgCampsStackedRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgCreepsStacked END) AS avgCreepsStackedRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgDeaths END) AS avgDeathsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgDenies END) AS avgDeniesRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgFirstbloodClaimed END) AS avgFirstbloodClaimedRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgGold END) AS avgGoldRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgGoldPerMin END) AS avgGoldPerMinRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgGoldSpent END) AS avgGoldSpentRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgHeroDamage END) AS avgHeroDamageRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgHeroHealing END) AS avgHeroHealingRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgKills END) AS avgKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgLastHits END) AS avgLastHitsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgLevel END) AS avgLevelRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgNetWorth END) AS avgNetWorthRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgRoshansKilled END) AS avgRoshansKilledRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgRunePickups END) AS avgRunePickupsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgXpPerMin END) AS avgXpPerMinRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgWin END) AS avgWinRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgTotalGold END) AS avgTotalGoldRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgTotalXp END) AS avgTotalXpRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgKillsPerMin END) AS avgKillsPerMinRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgKda END) AS avgKdaRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgNeutralKills END) AS avgNeutralKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgTowerKills END) AS avgTowerKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgCourierKills END) AS avgCourierKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgLaneKills END) AS avgLaneKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgHeroKills END) AS avgHeroKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgObserverKills END) AS avgObserverKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgSentryKills END) AS avgSentryKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgRoshanKills END) AS avgRoshanKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgNecronomiconKills END) AS avgNecronomiconKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgAncientKills END) AS avgAncientKillsRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgBuybackCount END) AS avgBuybackCountRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgObserverUses END) AS avgObserverUsesRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgSentryUses END) AS avgSentryUsesRadiant,
          AVG( CASE WHEN t1.is_radiant = TRUE THEN t2.avgLaneEfficiency END) AS avgLaneEfficiencyRadiant,

          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.qtRecency END) AS qtRecencyDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.qtMatches END) AS qtMatchesDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgAssists END) AS avgAssistsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgCampsStacked END) AS avgCampsStackedDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgCreepsStacked END) AS avgCreepsStackedDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgDeaths END) AS avgDeathsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgDenies END) AS avgDeniesDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgFirstbloodClaimed END) AS avgFirstbloodClaimedDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgGold END) AS avgGoldDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgGoldPerMin END) AS avgGoldPerMinDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgGoldSpent END) AS avgGoldSpentDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgHeroDamage END) AS avgHeroDamageDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgHeroHealing END) AS avgHeroHealingDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgKills END) AS avgKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgLastHits END) AS avgLastHitsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgLevel END) AS avgLevelDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgNetWorth END) AS avgNetWorthDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgRoshansKilled END) AS avgRoshansKilledDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgRunePickups END) AS avgRunePickupsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgXpPerMin END) AS avgXpPerMinDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgWin END) AS avgWinDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgTotalGold END) AS avgTotalGoldDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgTotalXp END) AS avgTotalXpDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgKillsPerMin END) AS avgKillsPerMinDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgKda END) AS avgKdaDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgNeutralKills END) AS avgNeutralKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgTowerKills END) AS avgTowerKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgCourierKills END) AS avgCourierKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgLaneKills END) AS avgLaneKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgHeroKills END) AS avgHeroKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgObserverKills END) AS avgObserverKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgSentryKills END) AS avgSentryKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgRoshanKills END) AS avgRoshanKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgNecronomiconKills END) AS avgNecronomiconKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgAncientKills END) AS avgAncientKillsDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgBuybackCount END) AS avgBuybackCountDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgObserverUses END) AS avgObserverUsesDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgSentryUses END) AS avgSentryUsesDire,
          AVG( CASE WHEN t1.is_radiant = FALSE THEN t2.avgLaneEfficiency END) AS avgLaneEfficiencyDire

  FROM tb_match_player AS t1

  INNER JOIN silver.dota.fs_player_summary as t2
  ON t1.dt_match = t2.dtReference
  AND t1.account_id = t2.idAccount

  GROUP BY t1.match_id

),

tb_join (

SELECT t1.match_id,
       t1.radiant_win,

       t2.avgWinHeroRandiant1,
       t2.avgWinHeroRandiant2,
       t2.avgWinHeroRandiant3,
       t2.avgWinHeroRandiant4,
       t2.avgWinHeroRandiant5,
       t2.avgKdaHeroRandiant1,
       t2.avgKdaHeroRandiant2,
       t2.avgKdaHeroRandiant3,
       t2.avgKdaHeroRandiant4,
       t2.avgKdaHeroRandiant5,
       t2.avgDeathsHeroRandiant1,
       t2.avgDeathsHeroRandiant2,
       t2.avgDeathsHeroRandiant3,
       t2.avgDeathsHeroRandiant4,
       t2.avgDeathsHeroRandiant5,
       t2.avgXpPerMinHeroRandiant1,
       t2.avgXpPerMinHeroRandiant2,
       t2.avgXpPerMinHeroRandiant3,
       t2.avgXpPerMinHeroRandiant4,
       t2.avgXpPerMinHeroRandiant5,
       t2.avgGoldPerMinHeroRandiant1,
       t2.avgGoldPerMinHeroRandiant2,
       t2.avgGoldPerMinHeroRandiant3,
       t2.avgGoldPerMinHeroRandiant4,
       t2.avgGoldPerMinHeroRandiant5,
       t2.avgWinHeroDire1,
       t2.avgWinHeroDire2,
       t2.avgWinHeroDire3,
       t2.avgWinHeroDire4,
       t2.avgWinHeroDire5,
       t2.avgKdaHeroDire1,
       t2.avgKdaHeroDire2,
       t2.avgKdaHeroDire3,
       t2.avgKdaHeroDire4,
       t2.avgKdaHeroDire5,
       t2.avgDeathsHeroDire1,
       t2.avgDeathsHeroDire2,
       t2.avgDeathsHeroDire3,
       t2.avgDeathsHeroDire4,
       t2.avgDeathsHeroDire5,
       t2.avgXpPerMinHeroDire1,
       t2.avgXpPerMinHeroDire2,
       t2.avgXpPerMinHeroDire3,
       t2.avgXpPerMinHeroDire4,
       t2.avgXpPerMinHeroDire5,
       t2.avgGoldPerMinHeroDire1,
       t2.avgGoldPerMinHeroDire2,
       t2.avgGoldPerMinHeroDire3,
       t2.avgGoldPerMinHeroDire4,
       t2.avgGoldPerMinHeroDire5,
       
       t3.qtRecencyRadiant,
       t3.qtMatchesRadiant,
       t3.avgAssistsRadiant,
       t3.avgCampsStackedRadiant,
       t3.avgCreepsStackedRadiant,
       t3.avgDeathsRadiant,
       t3.avgDeniesRadiant,
       t3.avgFirstbloodClaimedRadiant,
       t3.avgGoldRadiant,
       t3.avgGoldPerMinRadiant,
       t3.avgGoldSpentRadiant,
       t3.avgHeroDamageRadiant,
       t3.avgHeroHealingRadiant,
       t3.avgKillsRadiant,
       t3.avgLastHitsRadiant,
       t3.avgLevelRadiant,
       t3.avgNetWorthRadiant,
       t3.avgRoshansKilledRadiant,
       t3.avgRunePickupsRadiant,
       t3.avgXpPerMinRadiant,
       t3.avgWinRadiant,
       t3.avgTotalGoldRadiant,
       t3.avgTotalXpRadiant,
       t3.avgKillsPerMinRadiant,
       t3.avgKdaRadiant,
       t3.avgNeutralKillsRadiant,
       t3.avgTowerKillsRadiant,
       t3.avgCourierKillsRadiant,
       t3.avgLaneKillsRadiant,
       t3.avgHeroKillsRadiant,
       t3.avgObserverKillsRadiant,
       t3.avgSentryKillsRadiant,
       t3.avgRoshanKillsRadiant,
       t3.avgNecronomiconKillsRadiant,
       t3.avgAncientKillsRadiant,
       t3.avgBuybackCountRadiant,
       t3.avgObserverUsesRadiant,
       t3.avgSentryUsesRadiant,
       t3.avgLaneEfficiencyRadiant,
       
       t3.qtRecencyDire,
       t3.qtMatchesDire,
       t3.avgAssistsDire,
       t3.avgCampsStackedDire,
       t3.avgCreepsStackedDire,
       t3.avgDeathsDire,
       t3.avgDeniesDire,
       t3.avgFirstbloodClaimedDire,
       t3.avgGoldDire,
       t3.avgGoldPerMinDire,
       t3.avgGoldSpentDire,
       t3.avgHeroDamageDire,
       t3.avgHeroHealingDire,
       t3.avgKillsDire,
       t3.avgLastHitsDire,
       t3.avgLevelDire,
       t3.avgNetWorthDire,
       t3.avgRoshansKilledDire,
       t3.avgRunePickupsDire,
       t3.avgXpPerMinDire,
       t3.avgWinDire,
       t3.avgTotalGoldDire,
       t3.avgTotalXpDire,
       t3.avgKillsPerMinDire,
       t3.avgKdaDire,
       t3.avgNeutralKillsDire,
       t3.avgTowerKillsDire,
       t3.avgCourierKillsDire,
       t3.avgLaneKillsDire,
       t3.avgHeroKillsDire,
       t3.avgObserverKillsDire,
       t3.avgSentryKillsDire,
       t3.avgRoshanKillsDire,
       t3.avgNecronomiconKillsDire,
       t3.avgAncientKillsDire,
       t3.avgBuybackCountDire,
       t3.avgObserverUsesDire,
       t3.avgSentryUsesDire,
       t3.avgLaneEfficiencyDire

FROM tb_match as t1

LEFT JOIN tb_match_hero_summary as t2
ON t1.match_id = t2.match_id

INNER JOIN tb_summary_player AS t3
ON t1.match_id = t3.match_id

)

SELECT * FROM tb_join;
