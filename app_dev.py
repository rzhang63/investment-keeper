
import akshare as ak
from datetime import datetime

datetime_str = '2023-04-16'

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


print(out)