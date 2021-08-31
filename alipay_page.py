import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,inspect
from sqlalchemy.dialects.mysql import insert
import difflib

import datetime
import utils
import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector.pandas_tools import write_pandas


def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

def get_table_list():
    with conn.cursor() as cur:
        table_list = [entry[1] for entry in cur.execute('show tables').fetchall()]
    return table_list



# create fundXIRR table
def create_fundXIRR():
    with conn.cursor() as cur:
        cur.execute(
        'CREATE TABLE IF NOT EXISTS fundXIRR_{}(date VARCHAR(255),fund VARCHAR(255),xirr DOUBLE,PRIMARY KEY(date,fund))'.format(st.session_state['user']))


def dropcreate_alipay_transactions():
    with conn.cursor() as cur:
        cur.execute(
        'DROP TABLE IF EXISTS alipay_transactions_{}'.format(st.session_state['user']))
        cur.execute(
        'CREATE TABLE IF NOT EXISTS alipay_transactions_{}(create_time VARCHAR(255),amount DOUBLE,status VARCHAR(255),name VARCHAR(255),code VARCHAR(255))'.format(st.session_state['user']))


def dropcreate_alipay_asset():
    with conn.cursor() as cur:
        cur.execute(
        'DROP TABLE IF EXISTS alipay_asset_{}'.format(st.session_state['user']))
        cur.execute(
        'CREATE TABLE IF NOT EXISTS alipay_asset_{}(code VARCHAR(255),asset DOUBLE,date VARCHAR(255))'.format(st.session_state['user']))



# upsert fundXIRR table
def upsert_fundXIRR(fund_name,xirr):
    with conn.cursor() as cur:
        cur.execute('INSERT INTO fundXIRR_{} (date, fund, xirr) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE date=%s,fund=%s,xirr = %s'.format(st.session_state['user']),(str(datetime.date.today()),fund_name,xirr,str(datetime.date.today()),fund_name,xirr))


def load_snowflake_to_pandas(table_name):
    with conn.cursor() as cur:
        cur.execute('select * from {}'.format(table_name))
        df = cur.fetch_pandas_all()
    return df


def load_and_save_transaction_file(file):
    name2code,code2name = utils.getAllFundCode()

    # read in transaction details
    transactions_df = pd.read_csv(file, skiprows=[0, 1, 2, 3], encoding='gbk').dropna(thresh=10)
    transactions_df.columns = [c.strip() for c in transactions_df.columns]  # trim column names
    fund_names = [fund_name.strip()[5:5+fund_name.strip()[5:].find('-')] if '蚂蚁财富' in fund_name else 'NA'
                     for fund_name in transactions_df['商品名称'] ]
    transactions_df['基金名称'] = fund_names
    
    transactions_df = transactions_df[['交易创建时间','金额（元）','资金状态','基金名称']]
    transactions_df.columns = ["CREATE_TIME",'AMOUNT','STATUS','NAME']
    transactions_df = transactions_df[~transactions_df['NAME'].isin(['帮你投','NA'])]

    unique_fund_names = set(transactions_df['NAME'])
    OriginalName2MatchedCode = {}
    for n in unique_fund_names:
        OriginalName2MatchedCode[n] = utils.get_closest_fund_code(n,name2code)
    st.write(OriginalName2MatchedCode)

    transactions_df['CODE'] = transactions_df.apply(lambda row: OriginalName2MatchedCode[row['NAME']][:6], axis=1)
    

    dropcreate_alipay_transactions()
    write_pandas(
        conn=conn,
        df=transactions_df,
        table_name='ALIPAY_TRANSACTIONS_'+st.session_state['user'].upper()
    )
    return transactions_df

def load_and_save_asset_file(file):
    # read in asset values
    assets_df = pd.read_excel(file, skiprows=[0, 1, 2])[['基金代码','资产小计','单位净值日期']]
    assets_df.columns = ['CODE','ASSET','DATE']
    assets_df = assets_df.astype({'DATE': 'str'})
    assets_df['CODE'] = [str(int(c)) if len(str(int(c)))==6 else '0'*(6-len(str(int(c))))+str(int(c)) for c in assets_df['CODE'].tolist()]

    dropcreate_alipay_asset()
    write_pandas(
        conn=conn,
        df=assets_df,
        table_name='ALIPAY_ASSET_'+st.session_state['user'].upper()
    )

    return assets_df

def display_all_funds(transactions_df,assets_df):
    # get all fund code and name
    name2code,code2name = utils.getAllFundCode()

    unique_fund_code1 = set(transactions_df['CODE'])
    unique_fund_code2 = set(assets_df['CODE'])
    #st.write(unique_fund_code1)
    #st.write(unique_fund_code2)
    #st.write(unique_fund_names1.difference(unique_fund_names2))
    #st.write(unique_fund_names2.difference(unique_fund_names1))
    assert unique_fund_code2.issubset(unique_fund_code1)

    #unique_fund_names1 = set([code2name[c] for c in unique_fund_code1])
    #st.write(unique_fund_names1)
    
    # load xirr table from db
    #xirr_df = pd.read_sql('select * from fundXIRR_{}'.format(st.session_state['user']), con=engine)
    #st.write(xirr_df)

    for fund_code in unique_fund_code1:
        #if fund_name+'_xirr' not in st.session_state:
        #    st.session_state[fund_name+'_xirr'] = None
        column1, column2= st.columns(2)
        with column1:
            st.write(code2name[fund_code])
            st.write(fund_code)
        with column2:
            if fund_code in unique_fund_code2:
                asset_date = assets_df[assets_df['CODE']==fund_code]['DATE'].values.item()
                asset_value = assets_df[assets_df['CODE']==fund_code]['ASSET'].values.item()
            else:
                asset_date = str(datetime.date.today())
                asset_value = 0

            df = transactions_df[transactions_df['CODE'] == fund_code]
            #st.write(df)
            #date_list = [datetime.datetime.strptime(i.strip(), '%Y-%m-%d %H:%M:%S') for i in df['交易创建时间']]
            status_list = [1 if '收入' in s.strip() else -1 for s in df['STATUS']]
            amount_list = [x[0]*x[1] for x in list(zip(df['AMOUNT'].tolist(),status_list))]

            data = list(zip(df['CREATE_TIME'].tolist(), amount_list))
            data = [(x[0].strip(),x[1]) for x in data if x[0]<=asset_date+' 23:59:59'] + [(asset_date+' 23:59:59',asset_value)]
            data = [(datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S'),x[1]) for x in data]

            xirr = utils.xirr(data)
            st.write('XIRR: {}% ({})'.format(round(xirr*100,2),asset_date))

            #st.write(data)

            #+ [(datetime.datetime.now(),current_value)]
            #
            #st.session_state[fund_name+'_xirr'] = xirr
            #upsert_fundXIRR(fund_name,xirr)
            #
            
            
            #else:
            #    if st.session_state[fund_name+'_xirr']:
            #        st.write('XIRR: {}% ({})'.format(round(st.session_state[fund_name+'_xirr']*100,2),datetime.date.today()))
            #    else:
            #        if fund_name in set(xirr_df['fund']):
            #            latest_date = xirr_df[xirr_df['fund']==fund_name]['date'].values.item()
            #            latest_xirr = xirr_df[(xirr_df['fund']==fund_name) & (xirr_df['date']==latest_date)]['xirr'].values.item()
            #            st.write('XIRR: {}% ({})'.format(round(latest_xirr*100,2),latest_date))
            #        else:
            #            st.write('XIRR: ')
            #st.write(st.session_state[fund_name+'_xirr'])
    
    #st.write(pd.DataFrame(xirr_dict))
    
    

def main():
    st.header('支付宝')
    
    # upload files
    transaction_file = st.file_uploader("上传历史交易明细", type=['csv'])
    current_asset_file = st.file_uploader("上传最新资产证明", type=['xlsx'])

    # create table fundXIRR if not exists
    create_fundXIRR()

    # define table names
    asset_table_name = 'ALIPAY_ASSET_'+st.session_state['user'].upper()
    transaction_table_name = 'ALIPAY_TRANSACTIONS_'+st.session_state['user'].upper()

    # 检查是否上传了历史交易明细
    if transaction_file is not None and current_asset_file is not None: # 都上传了
        # read in uploaded files
        transactions_df = load_and_save_transaction_file(transaction_file)
        assets_df = load_and_save_asset_file(current_asset_file)

        display_all_funds(transactions_df,assets_df) 
    elif current_asset_file is not None: # 资产证明上传了，但是交易明细没上传
        # get existing tables in db
        table_list = get_table_list()

        # 检查数据库里是否有历史交易明细
        if transaction_table_name not in table_list:
            st.write('请上传历史交易明细')
        else:
            # read in uploaded asset file
            assets_df = load_and_save_asset_file(current_asset_file)

            # load transactions from db
            transactions_df = load_snowflake_to_pandas(transaction_table_name)
            
            #display_all_funds(transactions_df,assets_df)
    elif transaction_file is not None: # 资产证明没上传，但是交易明细上传了
        # get existing tables in db
        table_list = get_table_list()

        # 检查数据库里是否有资产证明
        if 'alipay_asset_'+st.session_state['user'] not in table_list:
            st.write('请上传最新资产证明')
        else:
            # read in transaction details from uploaded file
            transactions_df = load_and_save_transaction_file(transaction_file)

            # load asset values from db
            assets_df = load_snowflake_to_pandas(asset_table_name)

            #display_all_funds(transactions_df,assets_df)
    else: # 都没上传
        # get existing tables in db
        table_list = get_table_list()
        st.write(table_list)

        # 检查数据库里是否有资产证明
        if asset_table_name in table_list and transaction_table_name in table_list:
            assets_df = load_snowflake_to_pandas(asset_table_name)
            transactions_df = load_snowflake_to_pandas(transaction_table_name)
            #display_all_funds(transactions_df,assets_df)
        elif asset_table_name in table_list:
            st.write('请上传历史交易明细')
        elif transaction_table_name in table_list:
            st.write('请上传最新资产证明')
        else:
            st.write('请上传历史交易明细')
            st.write('请上传最新资产证明')


    
    
        


if __name__ == '__main__':
    main()
