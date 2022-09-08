-- Databricks notebook source
DROP TABLE IF EXISTS bronze_gamelakehouse.dota_teams ;

CREATE TABLE bronze_gamelakehouse.dota_teams 
WITH tb_teams AS (

  SELECT from_unixtime(start_time, 'yyyy-MM-dd') AS dt_match,
         dire_team.*
  FROM bronze_gamelakehouse.dota_match_details
  WHERE dire_team.team_id IS NOT null

  UNION ALL

  SELECT from_unixtime(start_time, 'yyyy-MM-dd') AS dt_match,
         radiant_team.*
  FROM bronze_gamelakehouse.dota_match_details
  WHERE radiant_team.team_id IS NOT null

)

SELECT team_id,
       name,
       tag,
       logo_url

FROM tb_teams
QUALIFY row_number() OVER (PARTITION BY team_id ORDER BY dt_match DESC) = 1
;
