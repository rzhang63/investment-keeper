import hashlib
from sre_constants import SUCCESS


import streamlit as st

from google.oauth2 import service_account
from gspread_pandas import Spread,Client
import pandas as pd
from datetime import datetime


#import huabao_page
import overview_page
#import alipay_page


st.set_page_config(layout="wide")





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
worksheet_list = sh.worksheets()



# Functions 
def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    if make_hashes(password) == hashed_text:
        return hashed_text
    return False

@st.cache()
# Get our worksheet names
def worksheet_names():
    sheet_names = []   
    for sheet in worksheet_list:
        sheet_names.append(sheet.title)  
    return sheet_names

# get worksheet as a dataframe
def load_worksheet(worksheetname):
    worksheet = sh.worksheet(worksheetname)
    df = pd.DataFrame(worksheet.get_all_records())
    return df

# update work sheet
def update_worksheet(worksheetname,df):
    col = df.columns
    spread.df_to_sheet(df[col],sheet = worksheetname,index = False)
    #st.sidebar.info('Updated to GoogleSheet')

def add_new_user(username,password):
    assert 'users' in worksheet_names()
    now = datetime.now()
    opt = {'username': [username],
           'password' :  [password]} 
    opt_df = pd.DataFrame(opt)

    old_users_df = load_worksheet('users')
    existing_users = old_users_df['username'].tolist()
    if username in existing_users:
        st.warning('User {} already exists. Try a different username.'.format(username))
    else:
        new_users_df = old_users_df.append(opt_df,ignore_index=True)
        update_worksheet('users',new_users_df)

def check_login(username,password):
    assert 'users' in worksheet_names()
    
    users_df = load_worksheet('users')
    user_pwd_pairs = users_df.values.tolist()

    is_login_ok = False
    for name,pwd in user_pwd_pairs:
        if name == username:
            is_login_ok = (pwd == password)
    
    return is_login_ok


def signout():
    # Delete all the items in Session state
    for key in st.session_state.keys():
        del st.session_state[key]
    st.experimental_rerun()


def main():
    # initialize count
    if 'login' not in st.session_state:
        st.session_state['login'] = 0

    #st.title("Investment Keeper")

    if st.session_state['login'] == 0:
        menu = ["Log In", "Sign Up"]
        choice = st.sidebar.selectbox("Menu", menu)
        if choice == "Log In":
            st.sidebar.header("Log In Section")

            username = st.sidebar.text_input("Username")
            password = st.sidebar.text_input("Password", type='password')
            if st.sidebar.button("Log In"):
                hashed_pwd = make_hashes(password)

                is_login_ok = check_login(username, check_hashes(password, hashed_pwd))
                if is_login_ok:
                    st.success("Logged In as {}".format(username))
                    st.session_state['login'] = 1
                    st.session_state['user'] = username
                    st.experimental_rerun()
                else:
                    st.warning("Incorrect Username/Password")
        else:
            st.sidebar.header("Create New Account")
            new_user = st.sidebar.text_input("Username")
            new_password = st.sidebar.text_input("Password", type='password')

            if st.sidebar.button("Sign Up"):
                add_new_user(new_user, make_hashes(new_password))
                st.success("You have successfully created a valid account")
                st.info("Go to Login Menu to login")
    else:
        PAGES = {
            "总览": overview_page
        }

        # user interaction
        st.sidebar.title('Navigation')
        selection = st.sidebar.radio("Go to", list(PAGES.keys()))
        page = PAGES[selection]
        page.main()

        st.sidebar.button('Sign out',on_click=signout)
    
    st.sidebar.markdown('<h5>Created by Ruotao Zhang</h5>', unsafe_allow_html=True)



if __name__ == '__main__':
    main()








