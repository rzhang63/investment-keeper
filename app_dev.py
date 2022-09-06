import hashlib


import streamlit as st

import huabao_page
import overview_page
import alipay_page
from google.oauth2 import service_account
from gsheetsdb import connect

st.set_page_config(layout="wide")




# Create a connection object.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)
conn = connect(credentials=credentials)

# Perform SQL query on the Google Sheet.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600)
def run_query(query):
    rows = conn.execute(query, headers=1)
    rows = rows.fetchall()
    return rows

sheet_url = st.secrets["private_gsheets_url"]
rows = run_query(f'SELECT * FROM "{sheet_url}"')

# Print results.
for row in rows:
    st.write(f"{row.name} has a :{row.pet}:")








def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    if make_hashes(password) == hashed_text:
        return hashed_text
    return False


# Initialize connection.
# Uses st.cache to only run once.
#@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()



# Perform query.
# Uses st.cache to only rerun when the query changes or after 10 min.
#@st.cache(ttl=600)
def run_query(sql,val=None):
    with conn.cursor() as cur:
        if val:
            cur.execute(sql,val)
        else:
            cur.execute(sql)
        return cur.fetchall()

def create_usertable():
    run_query('CREATE TABLE IF NOT EXISTS userstable(username VARCHAR(255) PRIMARY KEY,password VARCHAR(255))')


def add_userdata(username, password):
    # ADD: check if username already exists
    run_query('INSERT INTO userstable(username,password) VALUES (%s,%s)', (username, password))
    conn.commit()


def login_user(username, password):
    return run_query('SELECT * FROM userstable WHERE username =%s AND password = %s',(username, password))


def view_all_users():
    return run_query('SELECT * FROM userstable')

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
                create_usertable()
                hashed_pswd = make_hashes(password)

                result = login_user(
                    username, check_hashes(password, hashed_pswd))
                if result:
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
                create_usertable()
                if new_user in [r[0] for r in view_all_users()]:
                    st.warning('Username {} already exists. Try a different username.'.format(new_user))
                else:
                    add_userdata(new_user, make_hashes(new_password))
                    st.success("You have successfully created a valid account")
                    st.info("Go to Login Menu to login")
    else:
        PAGES = {
            "总览": overview_page
            ,"华宝": huabao_page
            ,"支付宝": alipay_page
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
