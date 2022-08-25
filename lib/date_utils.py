import datetime

def date_range(date_start, date_stop):
    
    datetime_start = datetime.datetime.strptime(date_start, '%Y-%m-%d')
    datetime_stop = datetime.datetime.strptime(date_stop, '%Y-%m-%d')
    
    dates = []
    
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime('%Y-%m-%d'))
        datetime_start += datetime.timedelta(days=1)
    
    return dates