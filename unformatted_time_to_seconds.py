def unformatted_time_to_seconds(time_str):
    def hhmmss_to_seconds(time_str):
        if ':' not in time_str:
            return 0
        hh, mm , ss = map(int, time_str.split(':')) 
        return ss + 60*(mm + 60*hh)

    def time_abbreviation_to_seconds(time_str):
        total_seconds = 0        
        if len(time_str) < 2:
            return total_seconds

        import re
        seconds_per_unit = {"s": 1, "sec": 1,
                            "m": 60, "mins": 60,
                            "h": 3600, "hrs": 3600,
                            "d": 86400, "ds": 86400, "days": 86400,
                            "w": 604800, "wk": 604800, "wks": 604800, 
                            "mo": 2629744, "mos": 2629744,
                            "y": 31556926, "yr": 31556926, "yrs": 31556926}
        time_str = time_str.lower()

        parsed_time = re.findall(r'\d+', time_str)
        parsed_time_units = re.findall(r'[a-z]+', time_str)
        if len(parsed_time) != len(parsed_time_units):
            raise Exception("Can't calculate due to invalid input time: " + time_str)
        for i in range(len(parsed_time)):
            total_seconds += int(parsed_time[i]) * seconds_per_unit[parsed_time_units[i]]  
        return total_seconds

    total_seconds = hhmmss_to_seconds(time_str)
    if total_seconds == 0:
        total_seconds = time_abbreviation_to_seconds(time_str)
    return total_seconds

from enum import Enum
TimeUnit = Enum('TimeUnit', ['SECOND', 'MINUTE', 'HOUR', 'DAY'])

def convert_seconds_to(desired_unit, seconds):
    if desired_unit == TimeUnit.SECOND.name:
        return seconds
    elif desired_unit == TimeUnit.MINUTE.name:
        return seconds/60
    elif desired_unit == TimeUnit.HOUR.name:
        return seconds/3600
    elif desired_unit == TimeUnit.DAY.name:
        return seconds/86400
    else:
        raise Exception("Unsupported time unit: " + desired_unit)
        
for i in range(len(df['Resolution Time'])):
    df_cell = df['Resolution Time'].iloc[i]
    desired_time_unit = TimeUnit.HOUR.name
    converted_time = 0
    if type(df_cell) is str:
        converted_time = convert_seconds_to(desired_time_unit, unformatted_time_to_seconds(df_cell))
    elif type(df_cell) is int:
        converted_time = convert_seconds_to(desired_time_unit, df_cell)
    else:
        raise Exception("Invalid time: " + df_cell)
    
    df['Resolution Time'].iloc[i] = round(converted_time, 2)

for i in range(len(df['Total Response Time'])):
    df_cell = df['Total Response Time'].iloc[i]
    desired_time_unit = TimeUnit.HOUR.name
    converted_time = 0
    if type(df_cell) is str:
        converted_time = convert_seconds_to(desired_time_unit, unformatted_time_to_seconds(df_cell))
    elif type(df_cell) is int:
        converted_time = convert_seconds_to(desired_time_unit, df_cell)
    else:
        raise Exception("Invalid time: " + df_cell)
    
    df['Total Response Time'].iloc[i] = round(converted_time, 2)


# Convert those features to a list with float datatype
df['Resolution Time'] = list(df['Resolution Time'])
df['Total Response Time'] = list(df['Total Response Time'])