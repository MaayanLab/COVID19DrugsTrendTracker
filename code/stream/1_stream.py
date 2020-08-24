import requests
import json
import time
from datetime import datetime
from threading import Thread
import os
import gzip
import calendar
from dotenv import load_dotenv
load_dotenv(verbose=True)

# get environment vars from .env
consumer_key = os.environ.get('CONSUMER_KEY')
consumer_secret = os.environ.get('CONSUMER_SECRET')

PTH = os.environ.get('PTH')


def get_bearer_token(key, secret):
    response = requests.post(
        "https://api.twitter.com/oauth2/token",
        auth=(key, secret),
        data={'grant_type': 'client_credentials'},
        headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})
    if response.status_code is not 200:
        raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))
    body = response.json()
    return body['access_token']


# Helper method that saves the tweets to a file at the specified path
def save_data(item):
    FOLDER = str(round(calendar.timegm(time.gmtime())/86400)) # day timestamp in unix
    os.makedirs(os.path.join(PTH,FOLDER), exist_ok=True)
    file_name = str(int(datetime.utcnow().timestamp() * 1e3))
    with gzip.open(os.path.join(PTH,FOLDER,file_name+".json.gz"), 'a+') as fout:
        fout.write(json.dumps(item).encode('utf-8'))
        fout.write('\n'.encode('utf-8'))


def stream_connect(partition):
    response = requests.get("https://api.twitter.com/labs/1/tweets/stream/covid19?partition={}".format(partition),
                            headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython",
                                     "Authorization": "Bearer {}".format(
                                         get_bearer_token(consumer_key, consumer_secret))},
                            stream=True)
    for response_line in response.iter_lines():
        if response_line:
            save_data(json.loads(response_line))


def main():
    timeout = 0
    while True:
        for partition in range(1, 5):
            Thread(target=stream_connect, args=(partition,)).start()
        time.sleep(2 ** timeout * 1000)
        timeout += 1


if __name__ == "__main__":
    main()
