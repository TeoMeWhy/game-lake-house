-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.team_players_actual;
CREATE TABLE silver.dota.team_players_actual AS (

  SELECT t1.account_id as idPlayer,
         t1.team.team_id as idTeam,
         t2.name as descTeamName,
         t2.tag as descTeamTag,
         t1.dt_match as dtTeamPlayer

  FROM bronze.dota.match_players as t1
  
  left join bronze.dota.teams as t2
  on t1.team.team_id = t2.team_id

  WHERE t1.team.team_id IS NOT null

  QUALIFY row_number() OVER (PARTITION BY team.team_id ORDER BY dt_match DESC) <= 5

)
