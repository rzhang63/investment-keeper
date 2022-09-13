import streamlit as st 
import utils
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from datetime import date, datetime


sh, spread, worksheet_names = utils.setup_connection()



def aggrid_interactive_table(df: pd.DataFrame):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_side_bar()

    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection


def compute_metrics(df,end_date_str,end_date_price):
    total_buy_amount = df[df['amount']<0]['amount'].abs().sum()
    total_buy_quantity = df[df['amount']<0]['quantity'].sum()
    total_sell_amount = df[df['amount']>0]['amount'].abs().sum()
    total_sell_quantity = df[df['amount']>0]['quantity'].sum()

    buy_avg_cost = total_buy_amount / total_buy_quantity
    sell_avg_price = total_sell_amount / total_sell_quantity

    hold_quantity = total_buy_quantity - total_sell_quantity
    endDate_market_value = hold_quantity * end_date_price #end date市值
    

    hold_avg_cost1 = buy_avg_cost # 持有成本=买入成本（不考虑已实现盈利或亏损）
    hold_return_amount1 = hold_quantity * (end_date_price - hold_avg_cost1) # 持有部分的收益（不考虑已实现盈利或亏损）
    hold_return_pct1 = (end_date_price - hold_avg_cost1) / hold_avg_cost1 # 持有部分的收益率（不考虑已实现盈利或亏损）


    hold_return_amount2 = total_sell_amount + endDate_market_value - total_buy_amount # 持有部分的收益（不考虑已实现盈利或亏损）
    hold_avg_cost2 = end_date_price - hold_return_amount2 / hold_quantity #累计持有成本（考虑已实现盈利或亏损）
    hold_return_pct2 = (end_date_price - hold_avg_cost2) / hold_avg_cost2 # 累计收益率（考虑已实现盈利或亏损）


    valuesPerDate_list = df[['date','amount']].values.tolist() + [(end_date_str,end_date_price*hold_quantity)]
    valuesPerDate_list = [(datetime.strptime(x[0], '%Y-%m-%d'),x[1]) for x in valuesPerDate_list]
    
    
    xirr = utils.xirr(valuesPerDate_list)

    return {'total_buy_amount': total_buy_amount
            ,'total_buy_quantity': total_buy_quantity
            ,'buy_avg_cost': buy_avg_cost
            ,'total_sell_amount': total_sell_amount
            ,'total_sell_quantity': total_sell_quantity
            ,'sell_avg_price': sell_avg_price
            ,'hold_quantity': hold_quantity
            ,'hold_avg_cost1': hold_avg_cost1
            ,'hold_return_amount1': hold_return_amount1
            ,'hold_return_pct1': hold_return_pct1 
            ,'hold_avg_cost2': hold_avg_cost2
            ,'hold_return_amount2': hold_return_amount2
            ,'hold_return_pct2': hold_return_pct2
            ,'xirr': xirr
            }


def main():
    st.subheader('输入交易记录')
    #st.sidebar.header('AAA')

    # upload files
    transaction_file = st.file_uploader("上传历史交易明细", type=['csv'])\

    with st.form("input_transaction"):
        st.write("Input transactions")
        cols = st.columns((1, 1, 1, 1))
        transaction_date = cols[0].date_input("交易日期")
        code = cols[1].text_input('证券编码').upper()
        amount = cols[2].number_input('交易金额',format='%f')
        quantity = cols[3].number_input('交易数量',format='%f')

        # Every form must have a submit button.
        submitted = st.form_submit_button("Submit")
        if submitted:
            st.write('Submitted!')
            st.write(st.session_state['user'])
            old_transaction_df = utils.load_worksheet('transactions',sh)
            opt = {'user': [st.session_state['user']]
                   ,'date': [transaction_date]
                   ,'code' : [code]
                   ,'amount': [amount]
                   ,'quantity': [quantity]
                  } 
            opt_df = pd.DataFrame(opt)
            new_transaction_df = old_transaction_df.append(opt_df,ignore_index=True)
            utils.update_worksheet('transactions',new_transaction_df,spread=spread)

    df = utils.load_worksheet('transactions',sh).sort_values(by=['date'])#.astype({'date': 'datetime'})
    #df['date'] = pd.to_datetime(df['date'],format='%Y-%m-%d')

    df = df.where(df["user"]==st.session_state['user'])
    with st.expander("All Transactions"):
        selection = aggrid_interactive_table(df=df)



    cols = st.columns((1, 1, 1, 1))

    start_date = cols[0].date_input("开始日期",value=datetime.strptime('2020-01-01','%Y-%m-%d'),max_value=datetime.now())
    end_date = cols[1].date_input("结束日期",max_value=datetime.now())
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    assert start_date_str <= end_date_str

    #code = cols[2].selectbox("证券编码", options=df['code'].unique())


    st.subheader("Overall")



    st.subheader("CHAU")
    
    code = 'CHAU'
    selected_df = df[(df['code']==code) & (df['date']>=start_date_str) & (df['date']<=end_date_str)]
    end_date_price = utils.get_closest_price('107.CHAU',end_date)
    #st.write(selected_df[['date','amount']].values.tolist())
    
    # Total
    
    total_buy_amount = selected_df[selected_df['amount']<0]['amount'].abs().sum()
    total_buy_quantity = selected_df[selected_df['amount']<0]['quantity'].sum()
    total_sell_amount = selected_df[selected_df['amount']>0]['amount'].abs().sum()
    total_sell_quantity = selected_df[selected_df['amount']>0]['quantity'].sum()

    buy_avg_cost = total_buy_amount / total_buy_quantity
    sell_avg_price = total_sell_amount / total_sell_quantity

    hold_quantity = total_buy_quantity - total_sell_quantity
    endDate_market_value = hold_quantity * end_date_price #end date市值
    

    hold_avg_cost1 = buy_avg_cost # 持有成本=买入成本（不考虑已实现盈利或亏损）
    hold_return_amount1 = hold_quantity * (end_date_price - hold_avg_cost1) # 持有部分的收益（不考虑已实现盈利或亏损）
    hold_return_pct1 = (end_date_price - hold_avg_cost1) / hold_avg_cost1 # 持有部分的收益率（不考虑已实现盈利或亏损）


    hold_return_amount2 = total_sell_amount + endDate_market_value - total_buy_amount # 持有部分的收益（不考虑已实现盈利或亏损）
    hold_avg_cost2 = end_date_price - hold_return_amount2 / hold_quantity #累计持有成本（考虑已实现盈利或亏损）
    hold_return_pct2 = (end_date_price - hold_avg_cost2) / hold_avg_cost2 # 累计收益率（考虑已实现盈利或亏损）


    valuesPerDate_list = selected_df[['date','amount']].values.tolist() + [(end_date_str,end_date_price*hold_quantity)]
    valuesPerDate_list = [(datetime.strptime(x[0], '%Y-%m-%d'),x[1]) for x in valuesPerDate_list]
    
    
    xirr = utils.xirr(valuesPerDate_list)

    cols = st.columns((1, 1, 1))
    cols[0].metric("总买入金额 (股数, 买入成本)", "${:.2f} ({:.2f}, ${:.2f})".format(total_buy_amount, total_buy_quantity, buy_avg_cost))
    cols[1].metric("总卖出金额 (股数, 卖出均价)", "${:.2f} ({:.2f}, ${:.2f})".format(total_sell_amount, total_sell_quantity, sell_avg_price))
    cols[2].metric("{}持有市值（股数，收盘价）".format(end_date_str), "${:.2f} ({:.2f}, ${:.2f})".format(end_date_price*hold_quantity, hold_quantity, end_date_price))
    
    cols = st.columns((1, 1, 1))
    cols[0].metric("持有成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(hold_avg_cost1, hold_return_amount1, 100.0*hold_return_pct1))
    cols[1].metric("累计成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(hold_avg_cost2, hold_return_amount2, 100.0*hold_return_pct2))
    cols[2].metric("XIRR", "{:.2f}%".format(xirr*100.0))

    
    st.subheader('BTC')
    code = 'BTC'
    selected_df = df[(df['code']==code) & (df['date']>=start_date_str) & (df['date']<=end_date_str)]
    end_date_price = utils.get_crypto_price(code,end_date)
    metrics = compute_metrics(selected_df,end_date_str,end_date_price)

    cols = st.columns((1, 1, 1))
    cols[0].metric("总买入金额 (股数, 买入成本)", "${:.2f} ({:.2f}, ${:.2f})".format(metrics['total_buy_amount'], metrics['total_buy_quantity'], metrics['buy_avg_cost']))
    cols[1].metric("总卖出金额 (股数, 卖出均价)", "${:.2f} ({:.2f}, ${:.2f})".format(metrics['total_sell_amount'], metrics['total_sell_quantity'], metrics['sell_avg_price']))
    cols[2].metric("{}持有市值（股数，收盘价）".format(end_date_str), "${:.2f} ({:.2f}, ${:.2f})".format(end_date_price*metrics['hold_quantity'], metrics['hold_quantity'], end_date_price))
    
    cols = st.columns((1, 1, 1))
    cols[0].metric("持有成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(metrics['hold_avg_cost1'], metrics['hold_return_amount1'], 100.0*metrics['hold_return_pct1']))
    cols[1].metric("累计成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(metrics['hold_avg_cost2'], metrics['hold_return_amount2'], 100.0*metrics['hold_return_pct2']))
    cols[2].metric("XIRR", "{:.2f}%".format(metrics['xirr']*100.0))


    st.subheader('DOGE')
    code = 'DOGE'
    selected_df = df[(df['code']==code) & (df['date']>=start_date_str) & (df['date']<=end_date_str)]
    end_date_price = utils.get_crypto_price(code,end_date)
    metrics = compute_metrics(selected_df,end_date_str,end_date_price)

    cols = st.columns((1, 1, 1))
    cols[0].metric("总买入金额 (股数, 买入成本)", "${:.2f} ({:.2f}, ${:.2f})".format(metrics['total_buy_amount'], metrics['total_buy_quantity'], metrics['buy_avg_cost']))
    cols[1].metric("总卖出金额 (股数, 卖出均价)", "${:.2f} ({:.2f}, ${:.2f})".format(metrics['total_sell_amount'], metrics['total_sell_quantity'], metrics['sell_avg_price']))
    cols[2].metric("{}持有市值（股数，收盘价）".format(end_date_str), "${:.2f} ({:.2f}, ${:.2f})".format(end_date_price*metrics['hold_quantity'], metrics['hold_quantity'], end_date_price))
    
    cols = st.columns((1, 1, 1))
    cols[0].metric("持有成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(metrics['hold_avg_cost1'], metrics['hold_return_amount1'], 100.0*metrics['hold_return_pct1']))
    cols[1].metric("累计成本(收益)", "${:.2f} (${:.2f}, {:.2f}%)".format(metrics['hold_avg_cost2'], metrics['hold_return_amount2'], 100.0*metrics['hold_return_pct2']))
    cols[2].metric("XIRR", "{:.2f}%".format(metrics['xirr']*100.0))

    st.write(metrics['hold_avg_cost1'])


if __name__ == '__main__':
    main()