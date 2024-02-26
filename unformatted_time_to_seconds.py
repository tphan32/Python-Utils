# -*- coding: utf-8 -*-
"""
Created on Mon Feb 26 22:05:51 2024

@author: pttin
"""

import re
def unformatted_time_to_seconds(time):
    time = time.lower()
    seconds_per_unit = {"s": 1, "sec": 1,
                        "m": 60, "mins": 60,
                        "h": 3600, "hrs": 3600,
                        "d": 86400, "ds": 86400, "days": 86400,
                        "w": 604800, "wk": 604800, "wks": 604800, 
                        "mo": 2629744, "mos": 2629744,
                        "y": 31556926, "yr": 31556926, "yrs": 31556926}
    total_seconds = 0
    parsed_time = re.findall(r'\d+', time)
    parsed_time_units = re.findall(r'[a-z]+', time)
    if len(parsed_time) != len(parsed_time_units):
        raise Exception("Can't calculate due to invalid input time: " + time)
    for i in range(len(parsed_time)):
        total_seconds += int(parsed_time[i]) * seconds_per_unit[parsed_time_units[i]]  
    return total_seconds

test1 = "3d"
test2 = "2d 1h"
test3 = "1w 1d"
test4 = "3mos 3w"
test5 = "2d 10h"
print(unformatted_time_to_seconds(test1))
print(unformatted_time_to_seconds(test2))
print(unformatted_time_to_seconds(test3))
print(unformatted_time_to_seconds(test5))
