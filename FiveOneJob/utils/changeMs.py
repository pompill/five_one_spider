import time


def change_ms(d):
    if len(d) == 10:
        date = time.mktime(time.strptime(d, '%Y-%m-%d')) * 1000
    else:
        date = time.mktime(time.strptime(d, '%Y-%m-%d %H:%M:%S')) * 1000
    return int(date)
