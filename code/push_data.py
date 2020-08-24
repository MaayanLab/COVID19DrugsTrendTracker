# Push tweets data to the Covid Drug and Gene Sets website

from dotenv import load_dotenv
import requests
import os
from datetime import datetime, timezone
import datetime
import pandas as pd

url = "https://amp.pharm.mssm.edu/covid19/drugsets"

# load var from .env file
load_dotenv()

PTH = os.environ.get('PTH')

f = open(os.path.join(PTH,'data/log.txt'))
FOLDER = f.readline()
f.close()

yesterday = str(datetime.datetime.fromtimestamp((int(FOLDER)-1)*86400).strftime('%m-%d-%Y'))

drugs = pd.read_csv(os.path.join(PTH,"data/output/COVID19DrugsTrendTracker/daily_reports", yesterday +".csv"))
drugs = drugs[drugs[yesterday]>2]
if len(drugs) > 50 and len(drugs)<180:
  drugs = drugs.iloc[:,0].tolist()
  drugs = list(set(drugs))
  drugs = "\n".join(drugs)

  yesterday = str(datetime.datetime.fromtimestamp((int(FOLDER)-1)*86400).strftime('%m/%d/%Y'))
  
  payload = {
    'source': 'https://github.com/MaayanLab/COVID19DrugsTrendTracker/blob/master/daily_reports',
    'drugSet': drugs,
    'descrShort': 'Top drugs tweeted with the term COVID-19 on ' + yesterday,
    'descrFull': 'Used the Twitter API to track tweets about drugs in context of COVID-19',
    'authorName': 'EnrichrBot',
    'authorEmail': '@Botenrichr',
    'twitter': 'on',
    'authorAff': 'ISMMS'
  }
  response = requests.request("POST", url, data=payload)
  print(response.text.encode('utf8'))
