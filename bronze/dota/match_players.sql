-- Databricks notebook source
DROP TABLE IF EXISTS bronze.dota.match_players;

CREATE TABLE bronze.dota.match_players

WITH tb AS (

  SELECT
    dire_team,
    radiant_team,
    explode(players) AS player
  FROM bronze_gamelakehouse.dota_match_details

)

SELECT 
       player.*,
       CASE WHEN player.isRadiant = TRUE THEN radiant_team ELSE dire_team END AS team,
       from_unixtime(player.start_time, 'yyyy-MM-dd HH:mm:ss') AS dt_match
FROM tb

;
