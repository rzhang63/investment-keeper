import datetime
import akshare as ak

import pandas as pd
import snowflake.connector
import streamlit as st
from snowflake.connector.pandas_tools import write_pandas

import utils
import pyxirr


def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"],client_session_keep_alive=True)

conn = init_connection()


def dropcreate_huabao_transactions():
    with conn.cursor() as cur:
        cur.execute(
        'DROP TABLE IF EXISTS huabao_transactions_{}'.format(st.session_state['user']))
        cur.execute(
        'CREATE TABLE IF NOT EXISTS huabao_transactions_{}(DATE VARCHAR(255),TIME VARCHAR(255),CODE VARCHAR(255),name VARCHAR(255),type VARCHAR(255),amount DOUBLE,ACCOUNT_CODE VARCHAR(255))'.format(st.session_state['user']))


def dropcreate_huabao_asset(date):
    with conn.cursor() as cur:
        cur.execute(
        'DROP TABLE IF EXISTS huabao_asset_{}_{}'.format(st.session_state['user'],date))
        cur.execute(
        'CREATE TABLE IF NOT EXISTS huabao_asset_{}_{}(CODE VARCHAR(255),NAME VARCHAR(255),QUANTITY VARCHAR(255),ASSET DOUBLE,DATE VARCHAR(255))'.format(st.session_state['user'],date))

def remove_last_R001(df,account_code):
    df2 = df[(df['CODE']=='131810') & (df['ACCOUNT_CODE']==account_code)]
    datetime_col = df2['DATE'] + df2['TIME']
    max_index = int(datetime_col[datetime_col == datetime_col.max()].index[0])
    if df.loc[max_index,'AMOUNT'] < 0:
        return df.drop(index=max_index)
    else:
        return df

@st.cache
def get_stockbond_info(ttl=86400):
    # 新股代码和申购代码的对应关系
    stock_em_xgsglb_df = ak.stock_em_xgsglb(market="全部股票")[['股票代码','申购代码']]
    stock_code = stock_em_xgsglb_df['股票代码'].tolist()
    stockipo_code = stock_em_xgsglb_df['申购代码'].tolist()
    ipo2stock = dict(zip(stockipo_code,stock_code))

    # 新债代码和申购代码的对应关系
    bond_zh_cov_df = ak.bond_zh_cov()[['债券代码','申购代码']]
    bond_code = bond_zh_cov_df['债券代码'].tolist()
    bondipo_code = bond_zh_cov_df['申购代码'].tolist()
    ipo2bond = dict(zip(bondipo_code,bond_code))

    return ipo2stock,ipo2bond


def load_and_save_transaction_file(files_list):

    ipo2stock,ipo2bond = get_stockbond_info()

    def handle_code(code):
        if code in ipo2stock.keys():
            return ipo2stock[code]
        elif code in ipo2bond.keys():
            return ipo2bond[code]
        else:
            return code

    # read in transaction details
    transactions_df_list = [pd.read_excel(file, dtype={'证券代码': str,'成交日期':str,'发生金额':float})[['成交日期','成交时间','证券代码','证券名称','委托类别','发生金额','股东代码']] for file in files_list]
    for i in range(len(transactions_df_list)):
        transactions_df_list[i].columns = ['DATE','TIME','CODE','NAME','TYPE','AMOUNT','ACCOUNT_CODE']
        transactions_df_list[i] = transactions_df_list[i][transactions_df_list[i]['AMOUNT'] != 0]
        transactions_df_list[i] = transactions_df_list[i][transactions_df_list[i]['TYPE'].isin(['买入', '卖出', '红利', '融券', '融券购回','中签扣款','其他'])]
        transactions_df_list[i]['CODE'] = transactions_df_list[i].apply(lambda row:handle_code(row['CODE']),axis=1)
        transactions_df_list[i] = transactions_df_list[i].set_index(['DATE','TIME','CODE','NAME','TYPE','ACCOUNT_CODE'])

    # load existing transactions from db
    table_list = utils.get_table_list(conn)
    if 'HUABAO_TRANSACTIONS_'+st.session_state['user'].upper() in table_list:
        existing_transactions_df = utils.load_snowflake_to_pandas('HUABAO_TRANSACTIONS_'+st.session_state['user'].upper(),conn).set_index(['DATE','TIME','CODE','NAME','TYPE','ACCOUNT_CODE'])
        transactions_df_list.append(existing_transactions_df)

    # merge (upsert) all transaction dataframes together
    transactions_df = transactions_df_list[-1]
    if len(transactions_df_list) > 1:
        for df in transactions_df_list[:-1]:
            transactions_df = pd.concat([transactions_df[~transactions_df.index.isin(df.index)], df])
    transactions_df = transactions_df.reset_index()

    st.write(transactions_df[transactions_df['CODE']=='131810'])
    
    # remove last R001 if amount is negative
    account_code_list = [c for c in transactions_df['ACCOUNT_CODE'].unique().tolist() if 'A' not in c]
    for c in account_code_list:
        transactions_df = remove_last_R001(transactions_df,c)
    st.write(transactions_df[transactions_df['CODE']=='131810'])
    code2name = dict(zip(transactions_df['CODE'].tolist(),transactions_df['NAME'].tolist()))
    
    dropcreate_huabao_transactions()
    write_pandas(
        conn=conn,
        df=transactions_df,
        table_name='HUABAO_TRANSACTIONS_'+st.session_state['user'].upper()
    )

    return transactions_df,code2name


def load_and_save_asset_file(file):
    asset_date = file.name[:8]

    # read in asset values
    assets_df = pd.read_excel(file,skiprows=[0,1,2],dtype={'证券代码': str})[['证券代码','证券名称','证券数量','最新市值']]
    assets_df.columns = ['CODE','NAME','QUANTITY','ASSET']
    assets_df['DATE'] = asset_date

    dropcreate_huabao_asset(asset_date)
    write_pandas(
        conn=conn,
        df=assets_df,
        table_name='HUABAO_ASSET_'+st.session_state['user'].upper()+'_'+asset_date
    )

    #st.write(assets_df)

    return assets_df



def display_all_stocks(transactions_df,assets_df,code2name):

    unique_stock_code1 = set(transactions_df['CODE'])
    unique_stock_code2 = set(assets_df['CODE'])
    #st.write(unique_stock_code1)
    #st.write(unique_stock_code2)
    #st.write(unique_stock_code1.difference(unique_stock_code2))
    #st.write(unique_stock_code2.difference(unique_stock_code1))
    assert unique_stock_code2.issubset(unique_stock_code1)

    output = []
    for stock_code in unique_stock_code1:
        #st.write(stock_code)
        if stock_code in unique_stock_code2:
            asset_date = assets_df[assets_df['CODE']==stock_code]['DATE'].values.item()
            asset_value = assets_df[assets_df['CODE']==stock_code]['ASSET'].values.item()
        else:
            asset_date = str(datetime.date.today().strftime("%Y%m%d"))
            asset_value = 0

        df = transactions_df[transactions_df['CODE'] == stock_code]
        #st.write(df)
        data = list(zip(df['DATE'].tolist(), df['AMOUNT'].tolist()))
        if stock_code == '131810':
            if data[-1][1]<0:
                data = data[:-1]
        data = [(x[0].strip(),x[1]) for x in data if x[0]<=asset_date] + [(asset_date,asset_value)]
        data = [(datetime.datetime.strptime(x[0], '%Y%m%d'),x[1]) for x in data]
        cumulative_gain = sum([x[1] for x in data])
        #st.write(data)

        xirr = pyxirr.xirr(data)
        if xirr > 100:
            xirr = 'NA'
        output.append((stock_code,asset_date,asset_value,xirr,round(cumulative_gain,2)))

    sorted_output = sorted(output,key=lambda x:x[2],reverse=True)
    for o in sorted_output:
        stock_code,asset_date,asset_value,xirr,cumulative_gain = o[0],o[1],o[2],o[3],o[4]
        column1, column2= st.columns(2)
        with column1:
            st.write('{} ({})'.format(code2name[stock_code],stock_code))
        with column2:
            st.write('日期: {}, 市值: {}, XIRR: {}%, 累计盈亏：{}'.format(asset_date,asset_value,xirr if xirr=='NA' else round(xirr*100,2),cumulative_gain))
    

def main():
    st.header('华宝')
    
    # upload files
    transaction_files = st.file_uploader("上传历史交割单", type=['xlsx'],accept_multiple_files=True)
    current_asset_file = st.file_uploader("上传最新资产", type=['xlsx'])

    # define table names
    asset_table_name = 'HUABAO_ASSET_'+st.session_state['user'].upper()
    transaction_table_name = 'HUABAO_TRANSACTIONS_'+st.session_state['user'].upper()

    # 检查是否上传了历史交易明细
    if transaction_files is not None and current_asset_file is not None: # 都上传了
        # read in uploaded files
        transactions_df,code2name = load_and_save_transaction_file(transaction_files)
        assets_df = load_and_save_asset_file(current_asset_file)

        display_all_stocks(transactions_df,assets_df,code2name)
    elif current_asset_file is not None: # 资产证明上传了，但是交易明细没上传
        # get existing tables in db
        table_list = utils.get_table_list(conn)

        # 检查数据库里是否有历史交易明细
        if transaction_table_name not in table_list:
            st.write('请上传历史交易明细')
        else:
            # read in uploaded asset file
            assets_df = load_and_save_asset_file(current_asset_file)

            # load transactions from db
            transactions_df = utils.load_snowflake_to_pandas(transaction_table_name,conn)
            
            display_all_stocks(transactions_df,assets_df)
    elif transaction_files is not None: # 资产证明没上传，但是交易明细上传了
        # get existing tables in db
        table_list = utils.get_table_list(conn)

        # 检查数据库里是否有资产证明
        if 'alipay_asset_'+st.session_state['user'] not in table_list:
            st.write('请上传最新资产证明')
        else:
            # read in transaction details from uploaded file
            transactions_df,code2name = load_and_save_transaction_file(transaction_files)

            # load asset values from db
            assets_df = utils.load_snowflake_to_pandas(asset_table_name,conn)

            display_all_stocks(transactions_df,assets_df)
    else: # 都没上传
        # get existing tables in db
        table_list = utils.get_table_list(conn)
        #st.write(table_list)

        # 检查数据库里是否有资产证明
        if asset_table_name in table_list and transaction_table_name in table_list:
            assets_df = utils.load_snowflake_to_pandas(asset_table_name,conn)
            transactions_df = utils.load_snowflake_to_pandas(transaction_table_name,conn)
            #st.write(transactions_df)
            display_all_stocks(transactions_df,assets_df)
        elif asset_table_name in table_list:
            st.write('请上传历史交易明细')
        elif transaction_table_name in table_list:
            st.write('请上传最新资产证明')
        else:
            st.write('请上传历史交易明细')
            st.write('请上传最新资产证明')

if __name__ == '__main__':
    main()