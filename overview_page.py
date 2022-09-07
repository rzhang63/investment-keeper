import streamlit as st 
import utils
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode


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
            opt = {'date': [transaction_date]
                   ,'code' : [code]
                   ,'amount': [amount]
                   ,'quantity': [quantity]
                  } 
            opt_df = pd.DataFrame(opt)
            new_transaction_df = old_transaction_df.append(opt_df,ignore_index=True)
            utils.update_worksheet('transactions',new_transaction_df,spread=spread)

    df = utils.load_worksheet('transactions',sh)
    df = df.where(df["user"]==st.session_state['user'])
    with st.expander("All Transactions"):
        selection = aggrid_interactive_table(df=df)

    
    st.subheader("Metrics")


if __name__ == '__main__':
    main()