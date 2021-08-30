import streamlit as st
import pandas as pd
from sqlalchemy import create_engine,inspect
from sqlalchemy.dialects.mysql import insert

import datetime
import utils

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=st.secrets["mysql"]['host'], db=st.secrets["mysql"]['database'], user=st.secrets["mysql"]['user'], pw=st.secrets["mysql"]['password']))
sqlalchemy_conn = engine.connect()

# create fundXIRR table
@st.cache()
def create_fundXIRR():
    engine.execute(
        'CREATE TABLE IF NOT EXISTS fundXIRR_{}(date VARCHAR(255),fund VARCHAR(255),xirr DOUBLE,PRIMARY KEY(date,fund))'.format(st.session_state['user']))

# upsert fundXIRR table
def upsert_fundXIRR(fund_name,xirr):
    engine.execute('INSERT INTO fundXIRR_{} (date, fund, xirr) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE date=%s,fund=%s,xirr = %s'.format(st.session_state['user']),(str(datetime.date.today()),fund_name,xirr,str(datetime.date.today()),fund_name,xirr))


def display_all_funds(transactions_df,asset_df):
    # get all fund code and name
    name2code,code2name = utils.getAllFundCode()

    unique_fund_names1 = set(transactions_df['基金名称'])
    unique_fund_names2 = set([code2name[c] for c in asset_df['基金代码']])
    assert unique_fund_names2.issubset(unique_fund_names1)

    # remove unwanted fund names
    to_delete = {'帮你投','NA'}
    if to_delete.issubset(unique_fund_names1):
        unique_fund_names1.difference_update(to_delete)
    
    # keep the columns we need
    transactions_df = transactions_df[['交易创建时间','金额（元）','资金状态','基金名称']]

    # load xirr table from db
    #xirr_df = pd.read_sql('select * from fundXIRR_{}'.format(st.session_state['user']), con=engine)
    #st.write(xirr_df)

    for fund_name in unique_fund_names1:
        if fund_name+'_xirr' not in st.session_state:
            st.session_state[fund_name+'_xirr'] = None
        column1, column2= st.columns(2)
        with column1:
            st.write(fund_name)
        with column2:
            df = transactions_df[transactions_df['基金名称'] == fund_name]
            #date_list = [datetime.datetime.strptime(i.strip(), '%Y-%m-%d %H:%M:%S') for i in df['交易创建时间']]
            status_list = [1 if '收入' in s.strip() else -1 for s in df['资金状态']]
            amount_list = [x[0]*x[1] for x in list(zip(df['金额（元）'].tolist(),status_list))]

            data = list(zip(df['交易创建时间'].tolist(), amount_list))
            st.write(data)

            #+ [(datetime.datetime.now(),current_value)]
            #xirr = utils.xirr(data)
            #st.session_state[fund_name+'_xirr'] = xirr
            #upsert_fundXIRR(fund_name,xirr)
            #st.write('XIRR: {}% ({})'.format(round(xirr*100,2),datetime.date.today()))
            
            
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

    # 检查是否上传了历史交易明细
    if transaction_file is not None and current_asset_file is not None: # 都上传了
        # read in transaction details
        transactions_df = pd.read_csv(transaction_file, skiprows=[0, 1, 2, 3], encoding='gbk').dropna(thresh=10)
        transactions_df.columns = [c.strip() for c in transactions_df.columns]  # trim column names
        fund_names = [fund_name.strip()[5:5+fund_name.strip()[5:].find('-')] if '蚂蚁财富' in fund_name else 'NA'
                         for fund_name in transactions_df['商品名称'] ]
        transactions_df['基金名称'] = fund_names
        transactions_df.to_sql('alipay_transactions_'+st.session_state['user'], con=engine, if_exists='replace') # save data to mysql

        # read in asset values
        assets_df = pd.read_excel(current_asset_file, skiprows=[0, 1, 2])[['基金代码','资产小计','单位净值日期']]
        assets_df.to_sql('alipay_asset_'+st.session_state['user'], con=engine, if_exists='replace') # save data to mysql

        #display_all_funds(transactions_df,assets_df) 
    elif current_asset_file is not None: # 资产证明上传了，但是交易明细没上传
        # get existing tables in db
        ins = inspect(engine)
        table_list = ins.get_table_names()

        # 检查数据库里是否有历史交易明细
        if 'alipay_transactions_'+st.session_state['user'] not in table_list:
            st.write('请上传历史交易明细')
        else:
            # read in asset values
            assets_df = pd.read_excel(current_asset_file, skiprows=[0, 1, 2])[['基金代码','资产小计','单位净值日期']]
            assets_df.to_sql('alipay_asset_'+st.session_state['user'], con=engine, if_exists='replace') # save data to mysql

            # load transactions from db
            transactions_df = pd.read_sql('select * from {}'.format('alipay_transactions_'+st.session_state['user']), con=engine)
            
            #display_all_funds(transactions_df,assets_df)
    elif transaction_file is not None: # 资产证明没上传，但是交易明细上传了
        # get existing tables in db
        ins = inspect(engine)
        table_list = ins.get_table_names()

        # 检查数据库里是否有资产证明
        if 'alipay_asset_'+st.session_state['user'] not in table_list:
            st.write('请上传最新资产证明')
        else:
            # read in transaction details
            transactions_df = pd.read_csv(transaction_file, skiprows=[0, 1, 2, 3], encoding='gbk').dropna(thresh=10)
            transactions_df.columns = [c.strip() for c in transactions_df.columns]  # trim column names
            fund_names = [fund_name.strip()[5:5+fund_name.strip()[5:].find('-')] if '蚂蚁财富' in fund_name else 'NA'
                             for fund_name in transactions_df['商品名称'] ]
            transactions_df['基金名称'] = fund_names
            transactions_df.to_sql('alipay_transactions_'+st.session_state['user'], con=engine, if_exists='replace') # save data to mysql

            # load asset values from db
            assets_df = pd.read_sql('select * from {}'.format('alipay_asset_'+st.session_state['user']), con=engine)

            #display_all_funds(transactions_df,assets_df)
    else: # 都没上传
        st.write('请上传历史交易明细')
        st.write('请上传最新资产证明')


    
    
        


if __name__ == '__main__':
    main()
