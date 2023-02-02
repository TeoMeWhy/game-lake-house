-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.last_seen_fs_player_hero_summary;
CREATE TABLE silver.dota.last_seen_fs_player_hero_summary
SELECT *
FROM silver.dota.fs_player_hero_summary
WHERE account_id IS NOT null
QUALIFY row_number() OVER (PARTITION BY account_id, hero_id ORDER BY dtReference DESC) = 1
;
