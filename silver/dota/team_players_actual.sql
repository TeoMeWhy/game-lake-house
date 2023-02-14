-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.team_players_actual;
CREATE TABLE silver.dota.team_players_actual AS (

  SELECT account_id,
         team.team_id,
         dt_match       

  FROM bronze.dota.match_players

  WHERE team.team_id IS NOT null

  QUALIFY row_number() OVER (PARTITION BY team.team_id ORDER BY dt_match DESC) <= 5

)
