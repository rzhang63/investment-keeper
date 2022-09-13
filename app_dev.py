
import akshare as ak
from datetime import datetime
import requests
import pandas as pd

datetime_str = '2022-09-11'

datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d')

day = datetime_object.strftime('%Y-%m-%d')


stock_us_hist_df = ak.stock_us_hist(symbol='107.CHAU', start_date="19700101", end_date="22220101", adjust="qfq")
print(stock_us_hist_df)

def get_closest_price(code,date):
    date = date.strftime('%Y-%m-%d')
    stock_us_hist_df = ak.stock_us_hist(symbol=code, start_date="19700101", end_date="22220101", adjust="qfq")
    closest_day = '1930-10-14'
    date_list = sorted(stock_us_hist_df['日期'].tolist())
    assert date >= date_list[0]
    for d in date_list:
        if d < date:
            closest_day = d
        elif d == date:
            return stock_us_hist_df[stock_us_hist_df['日期']==d]['收盘'].values.item()
        else:
            return stock_us_hist_df[stock_us_hist_df['日期']==closest_day]['收盘'].values.item()
    return stock_us_hist_df[stock_us_hist_df['日期']==closest_day]['收盘'].values.item()

out = get_closest_price('107.CHAU',datetime_object)


def get_crypto_price(symbol, date):
    date_str = date.strftime('%Y-%m-%d')
    api_key = 'YOUR API KEY'
    exchange = 'USD'
    days = 365*3
    api_url = f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym={exchange}&limit={days}&api_key={api_key}'
    raw = requests.get(api_url).json()
    df = pd.DataFrame(raw['Data']['Data'])[['time', 'high', 'low', 'open', 'close']]#.set_index('time')
    df['time'] = pd.to_datetime(df['time'], unit = 's')
    
    assert date in df['time'].tolist()
    return df[df['time']==date_str]['close'].values.item()
    #return df.sort_values(by=['time'],ascending=True)

out = get_crypto_price('DOGE',datetime_object)

print(out)