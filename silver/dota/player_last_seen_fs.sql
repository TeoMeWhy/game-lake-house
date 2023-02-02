-- Databricks notebook source
DROP TABLE IF EXISTS silver.dota.player_last_seen_fs;

CREATE TABLE silver.dota.player_last_seen_fs
SELECT *
FROM silver.dota.fs_player_summary
QUALIFY row_number() OVER (PARTITION BY idAccount ORDER BY dtReference DESC) = 1

;
