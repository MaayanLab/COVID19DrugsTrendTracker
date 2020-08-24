# process collected tweets and save only tweets that contain drug name from a list of drugs

import pandas as pd
import requests
import json
import gzip
import time
import shutil
import datetime
from datetime import datetime
from threading import Thread
import os
import calendar
from dotenv import load_dotenv
from os import listdir
from os.path import isfile, join
import subprocess
import psutil

load_dotenv(verbose=True)

PTH = os.environ.get('PTH')
Drug_PATH = os.environ.get('Drug_PATH')


def getdata(dataDic,field):
  tmp=""
  for d in dataDic: tmp=tmp+","+str(d[field])
  return(tmp[1:])


def has_handle(fpath):
  for proc in psutil.process_iter():
    try:
      for item in proc.open_files():
        if fpath == item.path:
          return True
    except Exception:
      pass
  return False


def read_json(FOLDER,file):
  fn = os.path.join(PTH,FOLDER,file)
  flag = True
  data = [[] for i in range(18)] #create empty list
  with gzip.open(fn) as fin:
    try:
      for line in fin:
        try:
          json_obj = json.loads(line)
          # tweet related info
          if 'full_text' in json_obj: # The actual UTF-8 text
            text = json_obj['full_text']
          else:
            text = json_obj['text']
          drug_info = [drug.strip() for drug in drugs if drug in text.lower() ]
          # if there is no drug name in the text - do not enter this info
          if len(drug_info) == 0:
            continue
          data[0].append(text)
          data[1].append(json_obj['user']['id_str'])
          data[2].append(json_obj['user']['screen_name'])
          data[3].append(json_obj['user']['followers_count'])
          data[4].append(json_obj['user']['friends_count'])
          data[5].append(json_obj['user']['statuses_count'])
          data[6].append(json_obj['user']['created_at'])
          data[7].append(json_obj['id_str']) # The string representation of the unique identifier for this Tweet.
          data[8].append(json_obj['in_reply_to_user_id_str'])  # string representation of the original Tweet’s author ID
          data[9].append(json_obj['in_reply_to_status_id_str']) # If the represented Tweet is a reply, this field will contain the string representation of the original Tweet’s ID
          data[10].append(json_obj['created_at']) # UTC time when this Tweet was created
          data[11].append(getdata(json_obj['entities']['hashtags'],'text'))
          data[12].append(getdata(json_obj['entities']['user_mentions'],'id_str'))
          mt=getdata(json_obj['entities']['user_mentions'],'screen_name')
          data[13].append(mt)
          #
          if 'retweeted_status' in json_obj:
            retweet_to_id=json_obj['retweeted_status']['user']['id_str']
            org_tweet_id=json_obj['retweeted_status']['id_str']
          else:
            retweet_to_id=""
            org_tweet_id=""
          #
          data[14].append(retweet_to_id)
          data[15].append(org_tweet_id)
          #
          if json_obj['in_reply_to_user_id_str']!=None:
            tweet_type='RE'
          else:
            if len(mt)>0:
              tweet_type='MT'
            else:
              tweet_type='TW'
          #
          if 'retweeted_status' in json_obj:
            tweet_type='RT'
          #
          data[16].append(tweet_type)
          data[17].append(';'.join(drug_info))
        except ValueError:
          print('Decoding JSON has failed')
        except Exception as e:
          print(e,'at line 92')
    except:
      pass
  df = pd.DataFrame({
    'text':data[0],
    'user_id':data[1],
    'Screen_name':data[2],
    'followers_count':data[3],
    'friends_count':data[4],
    'statuses_count':data[5],
    'user_created_at':data[6],
    'tweet_id':data[7],
    'in_reply_to_user_id':data[8],
    'in_reply_to_status_id':data[9],
    'tweet_created_at':data[10],
    'hashtags':data[11],
    'mentions_id':data[12],
    'mentions_screeName':data[13],
    'retweet_to_id':data[14], 
    'org_tweet_id':data[15],
    'tweet_type':data[16],
    'DrugSymbol': data[17]
    })
  return(df)

 
def read_drugs():
  drugs = pd.read_csv(os.path.join(Drug_PATH,"druglist.csv"),header = None)
  drugs = drugs.drop_duplicates(keep='last')
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
  drugs.columns = ['drug_name']
  drugs = drugs[~drugs['drug_name'].str.contains('zinc|Colchiceine|testosterone|vitamin', regex=True)]
  drugs = pd.DataFrame([ x for x in drugs['drug_name'] if x.lower() not in blacklist])
  drugs.columns = ['drug_name']
  drugs['drug_name'] = drugs['drug_name'].str.replace('"', '')
  drugs = drugs['drug_name'].str.lower().tolist()
  drugs = [ (' ' + drug + ' ') for drug in drugs if len(drug)<40] # detect only drug name
  return(drugs)


if __name__=="__main__":
  drugs = read_drugs()
  FOLDERs = [ x for x in os.listdir(PTH) if not x.endswith(".gz") ]   # get files in folder
  if len(FOLDERs) == 0: # if no files wait for 1min
    time.sleep(60)
  while(True):
    if len(FOLDERs) == 0:
      FOLDERs = [ x for x in os.listdir(PTH) if not x.endswith(".gz") ]
      continue
    FOLDER = FOLDERs[0]
    path = os.path.join(PTH,FOLDER)
    files = [ x for x in os.listdir(path) if x.endswith("json.gz") ] # get only json files
    if ( len(files) == 0 ) and ( int(round(calendar.timegm(time.gmtime())/86400)) > int(FOLDER) ): # if folder is empty delete folder
      shutil.rmtree(os.path.join(PTH,FOLDER))
    for file in files:
      print(file)
      while has_handle(os.path.join(PTH,FOLDER,file)): # if the file is in writing then, hold
        time.sleep(1)
      df = read_json(FOLDER,file)
      # write data to cummulative file
      if not os.path.isfile(os.path.join(PTH,FOLDER+"_full_data.csv.gz")):
        df.to_csv(os.path.join(PTH,FOLDER+"_full_data.csv.gz"), compression='gzip', index=False)
      else: # file exists so append without writing the header
        df.to_csv(os.path.join(PTH,FOLDER+"_full_data.csv.gz"), mode='a', header=None, compression='gzip', index=False)
      try:
        os.remove(os.path.join(PTH,FOLDER,file))
      except:
        pass
    # check for other folders
    FOLDERs = [ x for x in os.listdir(PTH) if not x.endswith(".gz") ]
print("done.")
