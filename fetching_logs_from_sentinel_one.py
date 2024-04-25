import requests
import time
import sys
import json
from datetime import datetime, timedelta
import concurrent.futures
import threading
import queue
from enum import Enum

api_token = 'eyJraWQiOiJ0b2tlblNpZ25pbmciLCJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ2b3ZhbmtoYW5oQHplbml0ZWNoY3MuY29tIiwiaXNzIjoiYXV0aG4tdXMtZWFzdC0xLXByb2QiLCJkZXBsb3ltZW50X2lkIjoiNzE4MjIiLCJ0eXBlIjoidXNlciIsImV4cCI6MTcxNTQyMTcxMCwianRpIjoiNjEyNzI2NmMtMDUzNy00MzJkLWIzYzEtODQ0ZDZhY2EzZDQwIn0.mg1RZqZ1swfjETKfZqd9L55NcG__xkD4oRfZwknmLmBhbleFptVAzj8J7NTQY52nB67vNgGeN7z_jP3_piVhtA'
headers = {'Authorization': f'ApiToken {api_token}'}
file_lock = threading.Lock()

FINISHED = "FINISHED"
NUM_WORKERS = 4
TIME_RANGE = 30
LOGS_LIMIT_PER_REQUEST = 1000

class StatusCode(Enum):
    SUCCESS = 200
    SERVICE_UNAVAILABLE = 503
    TOO_MANY_REQUESTS = 429

class SleepTimeInSec(Enum):
    ZERO = 0
    FIVE = 5
    FORTY = 40

class SentinelOneFunctionName(Enum):
    INITIATE_QUERY = "initiate_query"
    FETCH_LOG_EVENTS = "fetch_log_events"
    GET_QUERY_STATUS = "get_query_status"

# read data to json
def read_data_json(data, to_date):
    try:
        # current_time = datetime.now()
        # Convert the timestamp to a human-readable format for the filename
        # formatted_time = to_date.strftime('%Y-%m-%d_%H')
        to_date_datetime = datetime.strptime(to_date, '%Y-%m-%dT%H:%M:%S.%fZ')
        formatted_time = to_date_datetime.strftime('%Y-%m-%d_%H')
        # Construct the filename
        # filename = f'./logs/events_{to_date}.log'
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
    return handle_response(response, log_prefix, initiate_query, from_date, to_date)

# Function to send GET request using the queryId
def fetch_log_events(query_id, options):
    log_prefix = 'fetch_log_events'
    url = f'https://usea1-016.sentinelone.net/web/api/v2.1/dv/events'
    params = {'queryId': query_id, 'limit': options['limit'], 'skip': options['skip']}

    print(f'{log_prefix} started with queryId = {query_id}')
    response = requests.get(url, params=params, headers=headers)
    return handle_response(response, log_prefix, fetch_log_events, query_id, options)

def get_query_status(query_id):
    log_prefix = 'get_query_status'
    url = 'https://usea1-016.sentinelone.net/web/api/v2.1/dv/query-status'
    params = {'queryId': query_id}

    print(f'{log_prefix} started getting status for queryId = {query_id}')
    # Wait for the service to process the query
    time.sleep(SleepTimeInSec.FIVE.value)

    response = requests.get(url, params=params, headers=headers)
    return handle_response(response, log_prefix, get_query_status, query_id)

def handle_response(res, func_name, fun, *args):
    def retry(func_name, sleep_time, fun, *args):
        print(f'{func_name} retries after some seconds')
        time.sleep(sleep_time)
        return fun(*args)

    def handle_success_initiate_query(data):
        query_id = data['data']['queryId']
        print(f'{func_name} done with returned queryId = {query_id}')
        return query_id

    def handle_success_fetch_log_events(data):
        return data

    def handle_success_get_query_status(data, func, *args):
        progress_status, response_state = data['data']['progressStatus'], data['data']['responseState']
        if response_state != FINISHED:
            print(f'{func_name} query is being processed. Progress Status: {progress_status}')
            return fun(*args)
        else:
            print(f'{func_name} query execution is done. Query results are ready to be fetched')
            return FINISHED

    if func_name not in ["initiate_query", "fetch_log_events", "get_query_status"]:
        raise ValueError(f'Unknown function name: {func_name}')

    if res.status_code == StatusCode.SUCCESS.value:
        print(f'{func_name} done successfully')
        data = res.json()
        if func_name == SentinelOneFunctionName.INITIATE_QUERY.value:
            return handle_success_initiate_query(data)
        elif func_name == SentinelOneFunctionName.GET_QUERY_STATUS.value:
            return handle_success_get_query_status(data, fun, *args)
        elif func_name == SentinelOneFunctionName.FETCH_LOG_EVENTS.value:
            return handle_success_fetch_log_events(data)
    elif res.status_code == StatusCode.SERVICE_UNAVAILABLE.value:
        print(f'{func_name} got error {res.reason}. Going to wait for the service becomes available')
        return retry(func_name, SleepTimeInSec.FIVE.value, fun, *args)
    elif res.status_code == StatusCode.TOO_MANY_REQUESTS.value:
        print(f'{func_name} got too many requests. Wait some secs for the service to cool down')
        if func_name == SentinelOneFunctionName.INITIATE_QUERY.value:
            return retry(func_name, SleepTimeInSec.FORTY.value, fun, *args)
        elif func_name == SentinelOneFunctionName.GET_QUERY_STATUS.value:
            return retry(func_name, SleepTimeInSec.ZERO.value, fun, *args)
        else:
            return retry(func_name, SleepTimeInSec.FIVE.value, fun, *args)
    else:
        print(
            f'Got error in {func_name}: \
            Reason {res.reason} \
            Error code: {res.status_code}')
        raise

def producer(tasks, start_date, event):
    print("Producer starts")
    # Set up the beginning time interval
    from_date, to_date = increase_time_interval(start_date, TIME_RANGE)
    while True:
        print(f'\nFetching Data from {from_date} to {to_date}')
        query_id = initiate_query(from_date, to_date)

        # Wait for the query to be processed
        get_query_status(query_id)

        # Log events are ready
        data = fetch_log_events(query_id, {'skip': 0, 'limit': 1})
        number_items = data['pagination']['totalItems']
        print(f'Getting {number_items} items in total')

        # Set up tasks for consumers
        for i in range(0, number_items, LOGS_LIMIT_PER_REQUEST):
            print("Producer puts", {'queryId': query_id, 'skip': i})
            tasks.put({'queryId': query_id, 'skip': i, 'date': to_date})
        # Send signals to start the consumers if there are tasks
        if event.is_set() is False and not tasks.empty():
            event.set()
        
        from_date, to_date = increase_time_interval(to_date, TIME_RANGE)
    print("Producer exits")

def consumer(tasks, event):
    # Wait for producer to fill up the tasks queue
    event.wait()

    print("Consumer starts")
    while True:
        # Queue prevents consumers from busy-waiting
        if tasks.empty():
            print("Consumer waits due to no tasks :(")

        task = tasks.get()
        query_id, skip, date = task['queryId'], task['skip'], task['date']
        print(f"Consumer gets log events from {query_id} and skip {skip} first events")
        read_data_json(fetch_log_events(query_id, {'skip': skip, 'limit': LOGS_LIMIT_PER_REQUEST}), date)
    print("Consumer exits")

def main():
    # fromDate = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # time.sleep(TIME_RANGE)

    # Example to set the time range
    from_date = "2024-04-12T20:58:00.0Z"
    # to_date = "2024-04-10T20:58:30.0Z"
    # from_date, to_date = increase_time_interval(from_date, TIME_RANGE)

    tasks = queue.Queue()
    event = threading.Event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        executor.submit(producer, tasks, from_date, event)
        for _ in range(NUM_WORKERS-1):
            executor.submit(consumer, tasks, event)

if __name__ == "__main__":
    main()

# 3 min 16 sec for fetching 16530 items from 2024-04-10T20:58:00.0Z to 2024-04-10T20:58:30.0Z with 1 worker
# 1 min 52 sec for fetching 16530 items from 2024-04-10T20:58:00.0Z to 2024-04-10T20:58:30.0Z with 2 workers
# 1 min 15 sec for fetching 16530 items from 2024-04-10T20:58:00.0Z to 2024-04-10T20:58:30.0Z with 3 workers