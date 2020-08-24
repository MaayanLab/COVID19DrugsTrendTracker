# EnrichrBot tweet drugs
from dotenv import load_dotenv
import os, os.path
import pandas as pd
import random
import re
import requests
import sys
import time
import datetime
import tweepy
from bs4 import BeautifulSoup 
load_dotenv(verbose=True)

# get environment vars from .env
PTH = os.environ.get('PTH')

ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.environ.get('ACCESS_TOKEN_SECRET')
CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')

# Twitter authentication
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)

  
def main_tweet():
  # Construct the tweet
  f = open(os.path.join(PTH,'data/today_barplot.txt'))
  barplot = f.readline()
  f.close()
  barplot = barplot.replace('\n', '')
  barplot = barplot.replace('"', '')
  with open(os.path.join(PTH,"data/today_barplot.txt"), 'w+') as file:
    file.write('')
  message = "Experimental and approved drugs mentioned in the context of #COVID19 on {} collected by @EnrichrBot using the Twitter API.\n"
  message = message.format(barplot)
  message = message + "Access the cumulative reports: https://github.com/MaayanLab/COVID19DrugsTrendTracker\n\n"
  message = message + "@MaayanLab #Enrichr #COVID19cure https://amp.pharm.mssm.edu/covid19/"
  screenshots = [os.path.join(PTH,'data/output/COVID19DrugsTrendTracker/daily_barplots',barplot+'.png')]
  ps = [api.media_upload(screenshot) for screenshot in screenshots]
  media_ids = [p.media_id_string for p in ps]
  if '--dry-run' in sys.argv:
    print('tweet: {} {}'.format(message, media_ids))
  else: # post a reply
    try:
      api.update_status(media_ids=media_ids, status=message)
    except Exception as e:
      print(e)


if __name__ == '__main__':
  main_tweet()
