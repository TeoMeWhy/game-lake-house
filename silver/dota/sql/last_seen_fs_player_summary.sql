SELECT *
FROM silver.dota.fs_player_summary
QUALIFY row_number() OVER (PARTITION BY idAccount ORDER BY dtReference DESC) = 1