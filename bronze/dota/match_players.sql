-- Databricks notebook source
DROP TABLE IF EXISTS bronze_gamelakehouse.dota_match_players;

CREATE TABLE bronze_gamelakehouse.dota_match_players

WITH tb_players AS (
  SELECT explode(players) AS player
  FROM bronze_gamelakehouse.dota_match_details
)

SELECT player.*,
       from_unixtime(player.start_time, 'yyyy-MM-dd HH:mm:ss') AS dt_match
FROM tb_players
;
