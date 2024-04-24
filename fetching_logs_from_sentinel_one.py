import requests
import time
import sys
import json
from datetime import datetime, timedelta
import concurrent.futures
import threading



api_token = 'eyJraWQiOiJ0b2tlblNpZ25pbmciLCJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ2b3ZhbmtoYW5oQHplbml0ZWNoY3MuY29tIiwiaXNzIjoiYXV0aG4tdXMtZWFzdC0xLXByb2QiLCJkZXBsb3ltZW50X2lkIjoiNzE4MjIiLCJ0eXBlIjoidXNlciIsImV4cCI6MTcxNTQyMTcxMCwianRpIjoiNjEyNzI2NmMtMDUzNy00MzJkLWIzYzEtODQ0ZDZhY2EzZDQwIn0.mg1RZqZ1swfjETKfZqd9L55NcG__xkD4oRfZwknmLmBhbleFptVAzj8J7NTQY52nB67vNgGeN7z_jP3_piVhtA'
headers = {'Authorization': f'ApiToken {api_token}'}

file_lock = threading.Lock()

FINISHED = "FINISHED"
RETRY = "RETRY"
NUM_WORKERS = 2
TIME_RANGE = 20

def terminal_request_id(query_id):
    url = 'https://usea1-016.sentinelone.net/web/api/v2.1/dv/cancel-query'
    payload = {"queryId": query_id}
    headers = {'Authorization': f'ApiToken {api_token}'}
    # time.sleep(1)
    try:
        response = requests.post(
            url, json=payload, headers=headers)  # Indicate success
    except requests.exceptions.Timeout:
        return terminal_request_id(query_id)
    except requests.exceptions.RequestException as e:
        return terminal_request_id(query_id)
    if response.status_code == 200:
       # print("success")
        return 1  # Indicate success
    return -1  # Indicate failure

# read data to json
def read_data_json(data, toDate):
    try:
        # current_time = datetime.now()
        # Convert the timestamp to a human-readable format for the filename
        # formatted_time = toDate.strftime('%Y-%m-%d_%H')
        toDate_datetime = datetime.strptime(toDate, '%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_time = toDate_datetime.strftime('%Y-%m-%d_%H')
        # Construct the filename
        # filename = f'./logs/events_{toDate}.log'
        filename = f'events_{formatted_time}.log'
        print("Writing events to file")
        for obj in data['data']:
            # Write the JSON object to the file
            with file_lock:
                with open(filename, 'a') as file:
                    file.write(json.dumps(obj) + '\n')
    except IOError as e:
        print("Error writing to file:", e)


def increase_time_interval(to_date, time_range):
    new_to_date = calculate_new_to_date(to_date, time_range)
    return to_date, new_to_date

def calculate_new_to_date(from_date, time_range):
    to_date = datetime.strptime(
        from_date, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=time_range)
    return to_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Function to send POST request and extract queryId
def initiate_query(from_date, to_date):
    log_prefix = 'initiate_query'
    url = 'https://usea1-016.sentinelone.net//web/api/v2.1/dv/init-query'
    payload = {
        "accountIds": ["1530556909291787901"],
        "siteIds": ["1530556911045007055"],
        "fromDate": from_date,
        "limit": 20000,
        "queryType": ["events"],
        "toDate": to_date,
        "isVerbose": "false",
        # "timeFrame": "Last 100 Hours",
        "query": "AgentName IS NOT EMPTY",

    }

    print(f'{log_prefix} started to initiate the query and get queryId')
    response = requests.post(url, json=payload, headers=headers)
    data = handle_response(response, log_prefix, initiate_query, from_date, to_date)

    if data == RETRY:
        return initiate_query(from_date, to_date)

    query_id = data['data']['queryId']
    print(f'{log_prefix} done with returned queryId = {query_id}')
    return query_id

# Function to send GET request using the queryId
def fetch_log_events(query_id, options):
    skip, limit = options['skip'], options['limit']
    log_prefix = 'fetch_log_events'
    url = f'https://usea1-016.sentinelone.net/web/api/v2.1/dv/events'
    params = {'queryId': query_id, 'limit': limit, 'skip': skip}

    print(f'{log_prefix} started with queryId = {query_id}')
    response = requests.get(url, params=params, headers=headers)
    data = handle_response(response, log_prefix, fetch_log_events, query_id)

    if data == RETRY:
        return fetch_log_events(query_id, options)
    return data

# Function to send GET request using the queryId
def fetch_log_events_with_cursor(query_id, cursor):
    log_prefix = 'fetch_log_events_with_cursor'
    url = f'https://usea1-016.sentinelone.net/web/api/v2.1/dv/events'
    params = {'queryId': query_id, 'limit': 1000, 'cursor': cursor}

    print(f'{log_prefix} started with query_id = {query_id}')
    response = requests.get(url, params=params, headers=headers)
    data = handle_response(response, log_prefix, fetch_log_events_with_cursor, query_id, cursor)

    if data == RETRY:
        return fetch_log_events_with_cursor(query_id, cursor)
    return data

def get_query_status(query_id):
    log_prefix = 'get_query_status'
    url = 'https://usea1-016.sentinelone.net/web/api/v2.1/dv/query-status'
    params = {'queryId': query_id}

    print(f'{log_prefix} started getting status for queryId = {query_id}')
    # Wait for the service to process the query
    time.sleep(4)
    res = requests.get(url, params=params, headers=headers)
    data = handle_response(res, log_prefix, get_query_status, query_id)
    progress_status, response_state = data['data']['progressStatus'], data['data']['responseState']
    if response_state != FINISHED:
        print(f'{log_prefix} query is being processed. Progress Status: {progress_status}')
        return get_query_status(query_id)
    else:
        print(f'{log_prefix} query execution is done. Query results are ready to be fetched')
        return FINISHED

def handle_response(res, func_name, fun, *args):
    SHORT_TIME_SLEEP = 5
    LONG_TIME_SLEEP = 60
    if res.status_code == 200:
        print(f'{func_name} done successfully')
        return res.json()
    elif res.status_code == 503:
        print(f'{func_name} got error {res.reason}. Going to wait for the service becomes available')
        time.sleep(SHORT_TIME_SLEEP)
        print(f'{func_name} retries after some seconds')
        return RETRY
        # return fun(*args)
    elif res.status_code == 429:
        print(f'{func_name} got too many requests. Wait 1 min for the service to cool down')
        if (func_name == "initiate_query"):
            time.sleep(LONG_TIME_SLEEP)
        elif (func_name == "get_query_status"):
            # pass is intended
            pass
        else:
            time.sleep(SHORT_TIME_SLEEP)
        print(f'{func_name} retries after some times')
        # return fun(*args)
        # TODO
        return RETRY
    else:
        print(
            f'Got error in {func_name}: \
            Reason {res.reason} \
            Error code: {res.status_code}')
        raise

current_skip = None

def get_skip():
    global current_skip
    if current_skip is None:
        current_skip = 0
        return current_skip
    current_skip += 1000
    return current_skip

def process_query(query_id, skip_limit, to_date, lock):
    log_prefix = 'process_query'
    while True:
        lock.acquire()
        skip = get_skip()
        lock.release()
        if (skip >= skip_limit):
            break
        print(f'{log_prefix} Thread is processing {query_id} skip {skip} first items')
        logs = fetch_log_events(query_id, {'skip': skip, 'limit': 1000})
        read_data_json(logs, to_date)

# Main function
def main():
    # fromDate = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # time.sleep(20)
    # toDate = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'

    # 9357 items
    # fromDate = "2024-04-12T01:58:26.257525Z"
    # toDate = "2024-04-12T01:59:26.257525Z"

    from_date = "2024-04-10T20:58:00.0Z"
    # toDate = "2024-04-10T20:58:20.0Z"
    from_date, to_date = increase_time_interval(from_date, TIME_RANGE)

    lock = threading.Lock()

    # checker_f = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # checker_f = datetime.strptime(checker_f, "%Y-%m-%dT%H:%M:%S.%fZ")
    while True:
        print(f'\nFetching Data from {from_date} to {to_date}')

        query_id = initiate_query(from_date, to_date)

        # Wait for the query to be processed
        get_query_status(query_id)

        # events are ready
        data = fetch_log_events(query_id, {'skip': 0, 'limit': 1})

        number_items = data['pagination']['totalItems']
        print(f'Getting {number_items} items in total')
        skip_limit = number_items
        global current_skip
        current_skip = None

        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
            for i in range(0, NUM_WORKERS):
                executor.submit(process_query, query_id, skip_limit, to_date, lock)

        from_date, to_date = increase_time_interval(to_date, TIME_RANGE)

    # checker_t = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # checker_t = datetime.strptime(checker_t, "%Y-%m-%dT%H:%M:%S.%fZ")
    # if (checker_t - checker_f).total_seconds() < 60:
    #     sleep_time = 60 - (checker_t - checker_f).total_seconds()
    # # print(sleep_time)
    #     time.sleep(sleep_time)
    # # time.sleep(60)  # Wait before sending the next request batch


if __name__ == "__main__":
    main()


# eyJpZF9jb2x1bW4iOiAiJG9mZnNldCIsICJpZF92YWx1ZSI6IDEwMDAsICJpZF9zb3J0X29yZGVyIjogImFzYyIsICJzb3J0X2J5X2NvbHVtbiI6ICIkb2Zmc2V0IiwgInNvcnRfYnlfdmFsdWUiOiBudWxsLCAic29ydF9vcmRlciI6ICJhc2MifQ%3D%3D
# eyJpZF9jb2x1bW4iOiAiJG9mZnNldCIsICJpZF92YWx1ZSI6IDIwMDAsICJpZF9zb3J0X29yZGVyIjogImFzYyIsICJzb3J0X2J5X2NvbHVtbiI6ICIkb2Zmc2V0IiwgInNvcnRfYnlfdmFsdWUiOiBudWxsLCAic29ydF9vcmRlciI6ICJhc2MifQ%3D%3D
# eyJpZF9jb2x1bW4iOiAiJG9mZnNldCIsICJpZF92YWx1ZSI6IDMwMDAsICJpZF9zb3J0X29yZGVyIjogImFzYyIsICJzb3J0X2J5X2NvbHVtbiI6ICIkb2Zmc2V0IiwgInNvcnRfYnlfdmFsdWUiOiBudWxsLCAic29ydF9vcmRlciI6ICJhc2MifQ%3D%3D