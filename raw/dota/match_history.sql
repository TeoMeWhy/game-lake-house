SELECT *,
       from_unixtime(start_time, 'yyyy-MM-dd HH:mm:ss') as dt_start

FROM {table}