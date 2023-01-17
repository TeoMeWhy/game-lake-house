WITH tb_summary AS (

  SELECT account_id AS idAccount,
         MIN(DATEDIFF('{date}', dt_match)) AS qtRecency,
         COUNT(DISTINCT match_id) AS qtMatches,
         AVG(assists) AS avgAssists,
         AVG(camps_stacked) AS avgCampsStacked,
         AVG(creeps_stacked) AS avgCreepsStacked,
         AVG(deaths) AS avgDeaths,
         AVG(denies) AS avgDenies,
         AVG(firstblood_claimed) AS avgFirstbloodClaimed,
         AVG(gold) AS avgGold,
         AVG(gold_per_min) AS avgGoldPerMin,
         AVG(gold_spent) AS avgGoldSpent,
         AVG(hero_damage) AS avgHeroDamage,
         AVG(hero_healing) AS avgHeroHealing,
         AVG(kills) AS avgKills,
         AVG(last_hits) AS avgLastHits,
         AVG(level) AS avgLevel,
         AVG(net_worth) AS avgNetWorth,
         AVG(roshans_killed) AS avgRoshansKilled,
         AVG(rune_pickups) AS avgRunePickups,
         AVG(xp_per_min) AS avgXpPerMin,
         AVG(win) AS avgWin,
         AVG(total_gold) AS avgTotalGold,
         AVG(total_xp) AS avgTotalXp,
         AVG(kills_per_min) AS avgKillsPerMin,
         AVG(kda) AS avgKda,
         AVG(neutral_kills) AS avgNeutralKills,
         AVG(tower_kills) AS avgTowerKills,
         AVG(courier_kills) AS avgCourierKills,
         AVG(lane_kills) AS avgLaneKills,
         AVG(hero_kills) AS avgHeroKills,
         AVG(observer_kills) AS avgObserverKills,
         AVG(sentry_kills) AS avgSentryKills,
         AVG(roshan_kills) AS avgRoshanKills,
         AVG(necronomicon_kills) AS avgNecronomiconKills,
         AVG(ancient_kills) AS avgAncientKills,
         AVG(buyback_count) AS avgBuybackCount,
         AVG(observer_uses) AS avgObserverUses,
         AVG(sentry_uses) AS avgSentryUses,
         AVG(lane_efficiency) AS avgLaneEfficiency

  FROM bronze.dota.match_players

  WHERE dt_match < '{date}'
  AND dt_match >= date_add('{date}', -{lag})

  GROUP BY account_id
)

SELECT 
      DATE('{date}') AS dtReference,
      *

FROM tb_summary