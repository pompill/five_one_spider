import re


def change_to_k(num):
    if num % 1000 == 0:
        n = int(num/1000)
        newnum = str(n) + str('k')
    else:
        n = float(num/1000)
        totalnum = re.findall('(\d+)+\.', str(n))[0]
        newnum = str(totalnum) + str('k')
    return newnum

