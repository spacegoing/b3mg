# -*- coding: utf-8 -*-
from config import KeyId
import time
from pymongo import MongoClient
import pandas as pd
import requests
import traceback

client = MongoClient('mongodb://localhost:27017')
db = client['b3_db']
tradeinfo_col = db['tradeinfo']
error_col = db['error']

header = {
    'KeyId': KeyId
}

be_date = '2020-01-01'
today = pd.datetime.now().strftime('%Y-%m-%d')
date_list = pd.date_range(start='2020-01-01', end=today)
for d in date_list[1:]:
  next_url = 'https://api-marketdata-sandbox.b3.com.br/api/trade/v1.0/TradeInformation?SessionStartDate=%s&SessionEndDate=%s' % (
      be_date, d.strftime('%Y-%m-%d'))

  LastPageFlag = False
  while not LastPageFlag:
    try:
      print(next_url)
      res = requests.get(next_url, headers=header, verify=False)
      if res.status_code == 200:
        res = res.json()
        trade_list = res['Data']
        tradeinfo_col.insert_many(trade_list)
        LastPageFlag = res['Included']['LastPageIndicator']
        next_url = 'https://api-marketdata-sandbox.b3.com.br' + res[
            'Included'].get('NextPageURL', '')
        print('Successful: Inserted %d trades' % len(trade_list))
        time.sleep(2)
      else:
        error_col.insert({
            'exception': 'HTTP Error',
            'traceback': res.status_code,
            'url': next_url
        })

        print('HTTP Error: ', next_url)
        print('HTTP Code: ', res.status_code)
    except Exception as e:
      print(e)
      error_col.insert({
          'exception': str(e),
          'traceback': str(traceback.print_exc()),
          'url': next_url
      })
