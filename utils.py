import scipy.optimize
import requests
#import execjs
import difflib
import akshare as ak
import json
import hashlib
import streamlit as st
from google.oauth2 import service_account
from gspread_pandas import Spread,Client
import pandas as pd




def setup_connection():
    # Create a Google Authentication connection object
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    credentials = service_account.Credentials.from_service_account_info(
                    st.secrets["gcp_service_account"], scopes = scope)
    client = Client(scope=scope,creds=credentials)
    spreadsheetname = "data"
    spread = Spread(spreadsheetname,client = client)
    
    # Check the connection
    sh = client.open(spreadsheetname)
    worksheet_names = []   
    for sheet in sh.worksheets():
        worksheet_names.append(sheet.title)
    
    return sh, spread, worksheet_names


def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    if make_hashes(password) == hashed_text:
        return hashed_text
    return False


# get worksheet as a dataframe
def load_worksheet(worksheetname,sh):
    worksheet = sh.worksheet(worksheetname)
    df = pd.DataFrame(worksheet.get_all_records())
    return df


# update work sheet
def update_worksheet(worksheetname,df,spread):
    col = df.columns
    spread.df_to_sheet(df[col],sheet = worksheetname,index = False)
    #st.sidebar.info('Updated to GoogleSheet')


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


def get_table_list(conn):
    with conn.cursor() as cur:
        table_list = [entry[1] for entry in cur.execute('show tables').fetchall()]
    return table_list

def load_snowflake_to_pandas(table_name,conn):
    with conn.cursor() as cur:
        cur.execute('select * from {}'.format(table_name))
        df = cur.fetch_pandas_all()
    return df

def xnpv(valuesPerDate, rate):
    DAYS_PER_YEAR = 365.0
    assert isinstance(valuesPerDate, list)
    if rate == -1.0:
        return float('inf')

    #t0 = min(list(map(lambda x: x[0], valuesPerDate)))
    t0 = min(valuesPerDate,key=lambda x:x[0])[0]

    if rate <= -1.0:
        return sum([-abs(vi) / (-1.0 - rate) ** ((ti - t0).days / DAYS_PER_YEAR) for ti, vi in valuesPerDate])

    return sum([vi / (1.0 + rate) ** ((ti - t0).days / DAYS_PER_YEAR) for ti, vi in valuesPerDate])


def xirr(valuesPerDate):
    assert isinstance(valuesPerDate, list)
    if not valuesPerDate:
        return None
    list(map(lambda x: x[1], valuesPerDate))
    
    if all(x[1] >= 0 for  x in valuesPerDate):
        return float("inf")
    if all(x[1] <= 0 for  x in valuesPerDate):
        return -float("inf")

    result = None
    try:
        result = scipy.optimize.newton(lambda r: xnpv(valuesPerDate, r), 0)
    except (RuntimeError, OverflowError):  # Failed to converge?
        result = scipy.optimize.brentq(lambda r: xnpv(valuesPerDate, r), -0.999999999999999, 1e20, maxiter=10 ** 6)

    if not isinstance(result, complex):
        return result
    else:
        return None


def downloadAllFundCode():
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    content = requests.get(url)
    jsContent = execjs.compile(content.text)
    rawData = jsContent.eval('r')
    code2name = {}
    name2code = {}
    for x in rawData:
        code2name[x[0]] = x[2]
        name2code[x[2]] = x[0]
    
    with open("./name2code.json", "w") as outfile:
        json.dump(name2code, outfile)
    with open("./code2name.json", "w") as outfile:
        json.dump(code2name, outfile)

def getAllFundCode():
    with open('./name2code.json') as json_file:
        name2code = json.load(json_file)
    with open('./code2name.json') as json_file:
        code2name = json.load(json_file)
    return name2code,code2name

def get_closest_fund_code(s,name2code):
    matched_name = max([(k,difflib.SequenceMatcher(a=k, b=s).ratio()) for k in name2code.keys()],key=lambda x:x[1])[0]
    return name2code[matched_name]+matched_name
        
