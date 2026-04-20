import streamlit as st
import requests
import time
import pandas as pd
import matplotlib.pyplot as plt
import utils
import time
import os 
from datetime import  timedelta, timezone
import datetime

from dotenv import load_dotenv
load_dotenv()

# ---------------- CONFIG ----------------
DATABRICKS_INSTANCE = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID=os.getenv("JOB_ID")
AZURE_TENANT_ID = os.getenv("tenant_id")
AZURE_CLIENT_ID = os.getenv("client_id")
AZURE_CLIENT_SECRET = os.getenv("client_secret")
AZURE_SUBSCRIPTION_ID = os.getenv("subscription_id")

RESOURCE_GROUP = "RG-SpotifyAzureProject"
FACTORY_NAME = "adfspotifyazureprj"
PIPELINE_NAME = "github_analyzer_daily_pipeline"


DB_HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}


# ---------------- Welcome Window ----------------
if "show_dialog" not in st.session_state:
    st.session_state.show_dialog = True

# ---------------- Dialog ----------------
if st.session_state.show_dialog:
    # Create empty space to push content to center
    top_space = st.empty()
    
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        with st.container(border=True):
            st.markdown("## 👋 Welcome to GitHub Analyzer")
            st.write("""
            This app helps you:
            - Analyze GitHub archive data (from https://www.gharchive.org/ )
            - Run pipelines for a selected date  
            - View processed insights  

            **How to use:**
            1. Select a date  
            2. Wait for processing  
            3. View results  
            """)

            if st.button("Got it 👍"):
                st.session_state.show_dialog = False
                st.rerun()

    # Stop rest of app from rendering underneath
    st.stop()

# ---------------- UI ----------------
st.title("🚀 GitHub Archive Analyzer")


date = st.date_input(
    "Select Date",
    value=datetime.date(2015, 1, 1),
    min_value=datetime.date(2015, 1, 1), 
    # min_value=datetime.date(2011, 2, 12), # Date when the GH-Archive logs were first available
    # max_value=datetime.date(2030, 12, 31)
    max_value=datetime.date.today()
)
# hour = st.number_input("Hour", min_value=0, max_value=23, value=0)
# duration = st.number_input("Duration (hrs)", min_value=1, max_value=24, value=1)

# ---------------- BUTTON ----------------
if st.button("Analyze"):
    
    # token = utils.get_access_token(AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)

    # db_run_id = utils.get_databricks_run_id("36a7f273-2cc5-443d-8f01-b57a33045e45", token)

    # st.write(f"Databricks Run ID: {db_run_id}")
    # st.write(date.date.date)


    pipeline_start_time = datetime.datetime.now(timezone.utc)

    # ---------------- s Testing start ----------------
    # DONE : Before triggering the ADF pipeline, do check if the data for the selected date already exists in ADLS or not
    container_name = "githubanalyzer"
    date_path = f"year={date.year}/month={date.month}/day={date.day}"
    # st.write(date_path) # DEBUGGING
    directory_path_adls = f"bronze/{date}"
    exists_in_adls = utils.check_adls_data_exists(container_name, directory_path_adls, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
    directory_path_gold = f"gold/overview_table/{date_path}"
    exists_in_gold = utils.check_adls_data_exists(container_name, directory_path_gold, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
    # st.write(exists_in_adls) # DEBUGGING
    # st.write(exists_in_gold) # DEBUGGING

# --------------------------------------
    
    formatted_date = str(date)  # YYYY-MM-DD



# --------------------------------------

    # st.write(date.date.date) # DEBUGGING
    # st.success(f"Data exists in ADLS for {date} : {exists} |<br>| Skipping the Pipelines")
    # ---------------- s Testing end ----------------


    formatted_date = str(date)  # YYYY-MM-DD

    REFRESH_INTERVAL = 10
    runs = utils.get_active_runs()

    is_running = utils.is_pipeline_running_for_date(runs, formatted_date)

    if exists_in_gold:
        # TODO : If the Databricks pipeline is still running then the Data should not be displayed, it shoudld still say 'Data Processing' , so add a condition along with 'exists' only
        st.success(f"Data exists in ADLS for {date} : {exists_in_gold}. Skipping the Pipelines")
        loading_placeholder = st.empty()
        loading_placeholder.warning("Kindly Wait!!! Loading you data....")

    elif is_running:
        st.warning(f"Pipeline running for {formatted_date}... checking again in {REFRESH_INTERVAL}s")

        time.sleep(REFRESH_INTERVAL)
        st.rerun()

    else :

        # ---------------- s Testing start ----------------

        # Calling ADF Pipeline Trigger API
        # st.title("Trigger ADF Pipeline")
        token = utils.get_access_token(AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)

        response_of_api = utils.trigger_adf_pipeline(token, AZURE_SUBSCRIPTION_ID, RESOURCE_GROUP, FACTORY_NAME, PIPELINE_NAME, str(date))
        
        # st.write("Your ADF Pipeline Run ID response_of_api :") # DEBUGGING
        # st.write(response_of_api['runId']) # DEBUGGING

        st.success("ADF Pipeline Triggered!")

        # ---------------- s Testing end ----------------


        # ---------------- s Testing start ----------------
        # Polling ADF Pipeline Status 
        polling_status = utils.poll_adf_pipeline_status(  AZURE_SUBSCRIPTION_ID, RESOURCE_GROUP, FACTORY_NAME, response_of_api['runId'], token)


        # db_run_id = get_databricks_run_id("2cc97bbc-1daf-4e9b-b750-d43bcd7232b0", token)
        db_run_id = utils.get_databricks_run_id(response_of_api['runId'], token)

        # st.write(f"Databricks Run ID: {db_run_id}") # DEBUGGING
        
        # Running Databricks Job Status Polling
        utils.poll_databricks_pipeline_status(DATABRICKS_INSTANCE, DB_HEADERS, db_run_id)
        
        loading_placeholder = st.empty()
        loading_placeholder.warning("Kindly Wait!!! Loading you data....")

        # ---------------- s Testing end ----------------



    # ---------------- s Testing start ----------------
    # Reading data from ADLS Delta Table (using Databricks SQL Connector for Python) 
    # df = utils.read_data_from_adls(formatted_date, 'silver_raw', 'created_at', 'silver')

    # st.title("ADLS Delta Table Data")
    # st.dataframe(df)

    # ---------------- s Testing end ----------------



    # ---------------- s Testing start ----------------


    # -------------------------
    # BAR CHART (TREND)
    # -------------------------
    # Table 1 - 'total_events_per_hour'
    # UNCOMMENT-TODO
    df0 = utils.read_data_from_adls(formatted_date, 'total_events_per_hour')

    st.subheader("📈Events Trend throughout the day")
    # DONE : Try to fix the format of the hour here , or change it in the table from databricks, maybe using date_trunc   
    df0["hour"] = pd.to_datetime(df0["event_date"]) + pd.to_timedelta(df0["hour"], unit="h")
    utils.render_normal_chart(df0, x_col="hour", y_col="no_of_events")

    st.divider()  






    # -------------------------
    # METRICS (TOP HOURS)
    # -------------------------
    # UNCOMMENT-TODO
    df1 = utils.read_data_from_adls(formatted_date, 'total_events_per_hour')

    st.subheader("📊 Peak Hours")

    utils.show_peak_hours(df1, x_col="hour", y_col="no_of_events")

    st.divider()  






    # -------------------------
    # LINE CHART (TOP 3 REPO TREND THROUGHOUT THE DAY)
    # -------------------------
    # UNCOMMENT-TODO
    # DONE : How do you know which repo has topped that day 

    # Finding out the names of top 3 repos=======================================
    temp_query = f"""
    SELECT COUNT(repo_name) as no_of_events, repo_name
    FROM github_cata.silver.silver_raw
    WHERE DATE(created_at) = '{formatted_date}'
    GROUP BY repo_name
    ORDER BY no_of_events DESC
    LIMIT 3
    """
    temp_df = utils.extract_data_from_adls_for_query(temp_query)
    temp_df = temp_df['repo_name'].tolist()
    # ============================================================================

    df2 = utils.read_data_from_adls(formatted_date, 'top_3_repo_trend')
    df2 = df2[['hour'] + temp_df]
    df2 = df2.fillna(0)

    st.subheader("📈 Trend of TOP 3 REPO's THROUGHOUT THE DAY")
    
    utils.render_pivoted_chart(df2, legend_title="Top 3 Repos")

    st.divider()  






    # -------------------------
    # LINE CHART (TOP 5 ORG's TREND THROUGHOUT THE DAY)
    # -------------------------
    # UNCOMMENT-TODO
    # DONE - How do you know which repo has topped that day 

    # Finding out the names of top 3 orgs=======================================
    temp_query = f"""
    SELECT COUNT(org_name) as no_of_events, org_name
    FROM github_cata.silver.silver_raw
    WHERE DATE(created_at) = '{formatted_date}'
    GROUP BY org_name
    ORDER BY no_of_events DESC
    LIMIT 3
    """
    temp_df = utils.extract_data_from_adls_for_query(temp_query)
    temp_df = temp_df['org_name'].tolist()
    # ============================================================================

    df3 = utils.read_data_from_adls(formatted_date, 'top_3_org_trend')
    df3 = df3[['hour'] + temp_df] 
    df3 = df3.fillna(0)
    
    st.subheader("📈 Trend of TOP 3 ORG's THROUGHOUT THE DAY")
    utils.render_pivoted_chart(df3, legend_title="Top 3 Organizations")

    st.divider()  






    # -------------------------
    # CARDS (TOP 3 REPO's)
    # -------------------------
    extra_condition = """
        ORDER BY no_of_watchers DESC
        LIMIT 3
    """
    df4 = utils.read_data_from_adls(formatted_date, 'most_starred_repos', extra_conditions=extra_condition)
    df4 = df4[["repo_name", "no_of_watchers", "event_date"]]

    st.subheader("📈 TOP 3 MOST STARRED REPOS")

    utils.show_top_3_starred_repo(df4)

    st.divider()  




    # -------------------------
    # CARDS (TOP 3 REPO's by Pushes)
    # -------------------------
    df5 = utils.read_data_from_adls(formatted_date, 'top_3_repos_by_pushes')

    st.subheader("📈 TOP 3 MOST PUSHED REPOS")

    utils.show_top_3_repo_by_pushes(df5)

    st.divider()  






    # -------------------------
    # METRICS (TOP Top 5 Real Users by no_of_events_per_user)
    # -------------------------
    df6 = utils.read_data_from_adls(formatted_date, 'top_10_real_users', extra_conditions="ORDER BY no_of_events_per_user DESC LIMIT 5")

    st.subheader("🏆 Top 5 Real Users (Excluding Bots)")
    
    utils.display_top_users(df6)

    st.divider()





    # -------------------------
    # METRICS (Aggregated Metric for the day)
    # -------------------------

    df7 = utils.read_data_from_adls(formatted_date, 'overview_table')
    utils.display_daily_github_metrics(df7)
    

    pipeline_end_time = datetime.datetime.now(timezone.utc)

    total_time_taken = (pipeline_end_time - pipeline_start_time).total_seconds()
    st.write(total_time_taken)

    loading_placeholder.success("All the Data Loaded successfully.")
    time.sleep(5)
    loading_placeholder.empty()

#     st.write(date.date.date)


#     st.subheader(date.date.date)

#     # ---------------- s Testing end ----------------


#         # TODO : Build a input numeric field where you will pass no_of_top_repos to show in the line chart

#         # TODO : In DataBricks calculate Top 3 Public Repos, Private Repos

#         # TODO : Maybe you're picking the names of orgs as well while calculating top users, so do set org=Null while calculating top users

#         # TODO : Read all the [Read] or [Do Read] marked chats in the ChatGPT to prepare for interview 

#         # TODO : Include this point in Resume : (cache-first approach) because before triggering the databricks job, we are first checking if the data already exists in GitHub or not 

#         # TODO : While writing data to Volumes in Databricks, do write it using pandas dataframe, so first convert spark dataframe to pandas dataframe and then write it to volume. Pandas DF is much faster than Spark DF for small data.

#         # TODO : While discussing/researching about your architecture with chatGPT do discuss this point as well that how can we break our transformations or data into medallion architecture, and should we call the data finally which we're storing on the GitHub as "Gold Layer" , and if yes then what can be the "Silver Layer" and "Bronze Layer" in our case.

#         # TODO : Also include a functionality here at the top which will check if the date inputed is valid or whether the data for that date is available or not

#         # TODO : Try to save the API tokens in Azure key Vault

#         # TODO : Include ELT word in your resume.

#         # TODO : Your current pipeline have a caching/skip logic on the frontend side, but do include idempotency as well and include that point as well in the project README.



else:
    st.warning("Provide the Date and click Analyze")
    # st.warning("No data loaded")