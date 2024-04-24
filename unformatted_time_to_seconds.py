import requests
import time
import sys
import json
from datetime import datetime, timedelta

api_token = ''
# Update time interval every time request a batch fetching data (query ID)

headers = {'Authorization': f'ApiToken {api_token}'}

FINISHED = "FINISHED"
TIME_RANGE = 20


def update_time_interval(toDate, time_range):
    newToDate = calculate_to_date(toDate, time_range)
    return toDate, newToDate, time_range


def calculate_to_date(fromDate, time_range):
    toDate = datetime.strptime(
        fromDate, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(seconds=time_range)
    return toDate.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# Function to send POST request and extract queryId


def send_post_request(fromDate, toDate):
    url = 'https://usea1-016.sentinelone.net//web/api/v2.1/dv/init-query'
    payload = {
        "accountIds": [""],
        "siteIds": [""],
        "fromDate": fromDate,
        "limit": 20000,
        "queryType": ["events"],
        "toDate": toDate,
        "isVerbose": "false",
        # "timeFrame": "Last 100 Hours",
        "query": "AgentName IS NOT EMPTY",

    }
    headers = {'Authorization': f'ApiToken {api_token}'}
    print("send_post_request started to initate the query and get queryId")
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        data = response.json()
        query_id = data['data']['queryId']
        if query_id:
            print("send_post_request done")
            return query_id, fromDate, toDate
        else:
            print("Can't get queryId")
            raise
    elif response.status_code == 429:
        print("Too many requests. Wait 1 min for the service to cool down")
        time.sleep(60)
        raise
    else:
        print(
            f'Got error in send_post_request: \
            Reason {response.reason} \
            Error code: {response.status_code}')
        raise


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
            with open(filename, 'a') as file:
                file.write(json.dumps(obj) + '\n')
    except IOError as e:
        print("Error writing to file:", e)
# Function to send GET request using the queryId


def send_get_request(query_id):
    url = f'https://usea1-016.sentinelone.net/web/api/v2.1/dv/events'
    params = {'queryId': query_id, 'limit': 1000}
    print(f'send_get_request started with queryId = {query_id}')
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        print(f'send_get_request with queryId = {query_id} done')
        return response.json()
    else:
        print(
            f'Got error in send_get_request: \
            Reason {response.reason} \
            Error code: {response.status_code}')
        raise


# Function to send GET request using the queryId
def send_get_request_cursor(query_id, cursor):
    url = f'https://usea1-016.sentinelone.net/web/api/v2.1/dv/events'
    params = {'queryId': query_id, 'limit': 1000, 'cursor': cursor}

    print(f'send_get_request_cursor started with query_id = {query_id}')
    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 503:
        print(
            f'Got error {response.reason}. Going to wait for the service become available')
        time.sleep(5)
        return send_get_request_cursor(query_id, cursor)
    else:
        print(
            f'Got error in send_get_request_cursor: \
            Reason {response.reason} \
            Error code: {response.status_code}')
        raise


def get_query_status(query_id):
    url = 'https://usea1-016.sentinelone.net/web/api/v2.1/dv/query-status'
    params = {'queryId': query_id}
    print(f'Start getting status for queryId = {query_id}')
    # Wait for the service to process the query
    time.sleep(4)
    res = requests.get(url, params=params, headers=headers)
    if res.status_code == 200:
        data = res.json()['data']
        progressStatus, responseState = data['progressStatus'], data['responseState']
        if responseState != FINISHED:
            print(
                f'Query is being processed. Progress Status: {progressStatus}')
            return get_query_status(query_id)
        else:
            print("Query execution is done. Query results are ready to be fetched")
            return FINISHED
    else:
        print("Something went wrong in get_query_status")
        raise

# Main function


def main():
    # fromDate = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # time.sleep(20)
    # toDate = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'

    # 9357 items
    # fromDate = "2024-04-12T01:58:26.257525Z"
    # toDate = "2024-04-12T01:59:26.257525Z"

    # 20000 items
    fromDate = "2024-04-10T20:58:00.0Z"
    # toDate = "2024-04-10T20:58:20.0Z"
    toDate = calculate_to_date(fromDate, TIME_RANGE)

    # checker_f = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
    # checker_f = datetime.strptime(checker_f, "%Y-%m-%dT%H:%M:%S.%fZ")

    while True:
        print('Fetching Data In Range', fromDate, toDate)

        query_id, fromDate, toDate = send_post_request(fromDate, toDate)

        print("Received new queryId:", query_id)
        get_query_status(query_id)

        # events are ready
        data = send_get_request(query_id)

        number_items = data['pagination']['totalItems']
        print(f'Getting {number_items} items in total')
        while True:
            read_data_json(data, toDate)
            cursor = data['pagination']["nextCursor"]
            if cursor is None:
                break
            data = send_get_request_cursor(query_id, cursor)

        fromDate, toDate, time_range = update_time_interval(toDate, TIME_RANGE)

        # else:
        #     fromDate, toDate, time_range = update_time_interval_again(
        #         fromDate, toDate, time_range - 15)
        # data = send_get_request(query_id)
        # while data['pagination']['totalItems'] == 20000:
        #  print("Rerequest Query id")
        # terminal = terminal_request_id(query_id)
        #     fromDate, toDate, time_range = update_time_interval_again(
        #         fromDate, toDate, time_range - 15)
        #     query_id, fromDate, toDate, time_range = send_post_request(
        #         fromDate, toDate, time_range)
        #     data = send_get_request(query_id)
        #     # print("New time number ")
        #     # print(fromDate, toDate)
        #     # print(data['pagination']['totalItems'])
        #     # print("High s1 logs")
        # read_data_json(data, toDate)

        #terminal = terminal_request_id(query_id)
        # checker_t = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f") + 'Z'
        # checker_t = datetime.strptime(checker_t, "%Y-%m-%dT%H:%M:%S.%fZ")
        # if (checker_t - checker_f).total_seconds() < 60:
        #     sleep_time = 60 - (checker_t - checker_f).total_seconds()
        # # print(sleep_time)
        #     time.sleep(sleep_time)
        # # time.sleep(60)  # Wait before sending the next request batch


if __name__ == "__main__":
    main()
