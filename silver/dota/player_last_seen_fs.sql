-- Databricks notebook source
select *

from silver.dota.fs_player_summary

qualify row_number() over (partition by idAccount order by dtReference desc) = 1
