import streamlit as st
from os import path
from _config import config as _config_
from _util import _util_directory as _util_directory_




st.title('TubiBrick Job Submission Portal')

username = st.text_input("Sign In", placeholder='Enter your username')

ticket_number = st.text_input("Ticket Number", placeholder='Enter your job number')

sql_query = st.text_area("Paste your SQL query here", placeholder='Enter your SQL query here')

branch_name = st.text_input("Ticket Number", placeholder='Enter your job number', value="jian_dbt_poc")

model_name = st.text_input("Model Name", placeholder='Enter your model name', value="jian_poc_model")

if st.button("Submit Job"):
    if username and sql_query and ticket_number:

        _config = _config_.ConfigSingleton()
        task_dirpath = path.join(_config.TASK_HOME_DIR, ticket_number)
        _util_directory_.create_directory(task_dirpath)

        sql_filepath = path.join(_config.TASK_HOME_DIR, ticket_number)



