import hashlib
import os
import sqlite3
import overview_page

import pandas as pd
import streamlit as st


def make_hashes(password):
    return hashlib.sha256(str.encode(password)).hexdigest()


def check_hashes(password, hashed_text):
    if make_hashes(password) == hashed_text:
        return hashed_text
    return False


conn = sqlite3.connect('data.db')
c = conn.cursor()


def create_usertable():
    c.execute('CREATE TABLE IF NOT EXISTS userstable(username TEXT,password TEXT)')


def add_userdata(username, password):
    c.execute('INSERT INTO userstable(username,password) VALUES (?,?)',
              (username, password))
    conn.commit()


def login_user(username, password):
    c.execute('SELECT * FROM userstable WHERE username =? AND password = ?',
              (username, password))
    data = c.fetchall()
    return data


def view_all_users():
    c.execute('SELECT * FROM userstable')
    data = c.fetchall()
    return data


def main():
    st.title("Investment Keeper")

    menu = ["Login", "SignUp"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Login":
        st.sidebar.header("Login Section")

        username = st.sidebar.text_input("User Name")
        password = st.sidebar.text_input("Password", type='password')
        if st.sidebar.button("Login"):
            # if password == '12345':
            create_usertable()
            hashed_pswd = make_hashes(password)

            result = login_user(username, check_hashes(password, hashed_pswd))
            if result:
                st.success("Logged In as {}".format(username))

                PAGES = {
                    "总览": overview_page,
                }

                # user interaction 
                st.sidebar.title('Navigation')
                selection = st.sidebar.radio("Go to", list(PAGES.keys()))
                page = PAGES[selection]

                page.main()
            else:
                st.warning("Incorrect Username/Password")
    else:
        st.sidebar.header("Create New Account")
        new_user = st.sidebar.text_input("Username")
        new_password = st.sidebar.text_input("Password", type='password')

        if st.sidebar.button("Signup"):
            create_usertable()
            add_userdata(new_user, make_hashes(new_password))
            st.success("You have successfully created a valid Account")
            st.info("Go to Login Menu to login")


# st.write(os.getcwd())
#
#uploaded_file = st.file_uploader("Choose a file")
#
# if uploaded_file is not None:
#    #dataframe = pd.read_csv(uploaded_file,skiprows=[0,1,2,3],encoding='gbk')
#    dataframe = pd.read_excel(uploaded_file)
#    st.write(dataframe)
#    #with open(os.path.join("tempDir",image_file.name),"wb") as f:
#    #    f.write(image_file.getbuffer())
#    #st.success("Saved File")

if __name__ == '__main__':
    main()
