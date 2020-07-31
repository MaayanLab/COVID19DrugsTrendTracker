from dotenv import load_dotenv
import tweepy
import pandas as pd
import numpy as np
import datetime
from datetime import datetime, date, timedelta
from time import gmtime, strftime
import subprocess
import time
import os, os.path
import math
import threading
import json
import gzip
import sys
import calendar
import re
from pathlib import Path

# load var from .env file
load_dotenv()
PTH = os.environ.get('PTH')

#================================================================
# open a new directory every Monday
#================================================================
# open a new folder every day that contain the json tweets
FOLDER = str(round(calendar.timegm(time.gmtime())/86400))
os.makedirs(os.path.join(PTH,"data/tweets",FOLDER), exist_ok=True)
  
# write the curent folder name to file
with open(os.path.join(PTH,"data/log.txt"), 'w+') as file:
  file.write(FOLDER)

#================================================================
# collect tweets in parallel
#================================================================

#----- Help Function to download and save json files ------------

def Func2(i,dat,api,writepath):
  start_time = datetime.now()
  print('user', i, 'started at',start_time)
  d = str(date.today() - timedelta(days=1))
  for j in range(0,len(dat)):
    drug = dat['drug_name'].iloc[j]
    filename = os.path.join(writepath,drug+".json.gz")
    # collect all tweets from today
    for tweet in tweepy.Cursor(api.search, q=drug, since=d, until=str(date.today()), tweet_mode='extended',lang='en').items():
      with gzip.open(filename, 'a+') as fout:
        fout.write(json.dumps(tweet._json).encode('utf-8'))
        fout.write('\n'.encode('utf-8'))
      if datetime.now() > start_time + timedelta(seconds=10800): # after 3 hr stop collecting
        print("stopped user",i)
        return('')
  print("finished "+str(i))
  
#--------------------- End Help Function -----------------------
df = pd.read_csv( os.path.join(PTH,"data/druglist.csv"),header = None)
df = df.drop_duplicates(keep='last')

blacklist= [
            'nhc','(s,r)-fidarestat','ethanol','fit','an-9','w-9','w-5','w-12','cdc',	'oxygen','w-13','t-2000','tmr','yo-2','selenium','rita','cocaine','h-9','at-001',
              'w 12','poly-I:CLC','Y-134','SAL-1','ITE','PAC-1','Morphine','NM-3','PPT','s 23', 'T-62','Fica','GET-73','PP-2','PP-3','Xenon','DPN','ML-7','SC-10','obaa',
              'TAS-116','SANT-1','silver','heat','nicotine','ppl-100','pit','Ozone','sucrose','Copper','oxygen','an-9','cdc','fit','w-5','w-12','testosterone','atp',
              'platinum','bia','melatonin','sc-9','neon','acetate','serotonin','chlorine','chlorine-dioxide','acrylic','hydrogen-peroxide','nitric-oxide','saccharin'
              ,'psicose','propylene glycol','cobalt','silicon','asiatic','hypochlorite','nadh','neca','nitrous','citric','chloroform','carfentanil','iron','cobalt',
              'carbon monoxide','carbon monoxide','carbon-monoxide','psilocybin','hydrogen peroxide','titanium','ammonium' 'chloride','caffeine','calcium','cholesterol',
              'cortisone','donu','alcohol','lactose','aluminium','cccp','serine','piperinem','xylitol','zinc','barium','iodine','nitrate','fccp','bntx'
              ]

blacklist = [ '"'+ x.lower() +'"' for x in blacklist ]
df.columns = ['drug_name']
df = df[~df['drug_name'].str.contains('zinc|Colchiceine|testosterone|vitamin', regex=True)]

df = pd.DataFrame([ x for x in df['drug_name'] if x.lower() not in blacklist])
df.columns = ['drug_name']

# collect tweets
Number_of_apps= os.environ.get('APPS')
path = os.path.join(PTH,"data/tweets",FOLDER)
x=math.ceil(len(df['drug_name'])/Number_of_apps)
threads = []

for i in range(1,Number_of_apps+1): # number of api
  # proccess in threads
  dat=df[(i - 1) * x:i * x]
  if(len(dat)==0):
    continue
  else:
    APP_KEY = os.environ.get('APP_KEY'+str(i))
    APP_SECRET = os.environ.get('APP_SECRET'+str(i))
    auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)  
    api = tweepy.API(auth, wait_on_rate_limit=True)
    auth = tweepy.OAuthHandler(APP_KEY, APP_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    # start a threading tasks
    t = threading.Thread(target=Func2, args=(i,dat,api,path))
    threads.append(t)
    t.start()
    time.sleep(1)

print("Exiting Main Thread")
for x in threads:
  x.join()
