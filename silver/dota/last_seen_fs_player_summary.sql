-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.last_seen_fs_player_summary;
CREATE TABLE silver.dota.last_seen_fs_player_summary
SELECT *
FROM silver.dota.fs_player_summary
QUALIFY row_number() OVER (PARTITION BY idAccount ORDER BY dtReference DESC) = 1
;
