from urllib import response

import streamlit as st
import pandas as pd
import requests
import os

GITHUB_API = "https://api.github.com"
REPO_OWNER = "Siddheshbn"
REPO_NAME = "trying_to_write_db_to_github"
BRANCH = "main"


# -------------------------
# UTILITY Functions
# -------------------------
def get_access_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://management.azure.com/.default"
    }

    response = requests.post(url, data=payload)
    return response.json()["access_token"]
        
        
def trigger_adf_pipeline(token, AZURE_SUBSCRIPTION_ID, RESOURCE_GROUP, FACTORY_NAME, PIPELINE_NAME, date):
    subscription_id = AZURE_SUBSCRIPTION_ID
    resource_group = RESOURCE_GROUP
    factory_name = FACTORY_NAME
    pipeline_name = PIPELINE_NAME

    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 👇 PARAMETERS
    body = {   
        "p_date": date 
    }

    response = requests.post(url, headers=headers, json=body)

    # DEBUGGING
    # st.write(response.status_code)
    # st.write(response.text) 
    # st.write("This is your response")
    # st.write(response) 
    # st.write(response.json())
    # return response.status_code
    return response.json()


import os 
from datetime import  timedelta, timezone
import datetime


AZURE_TENANT_ID = os.getenv("tenant_id")
AZURE_CLIENT_ID = os.getenv("client_id")
AZURE_CLIENT_SECRET = os.getenv("client_secret")
AZURE_SUBSCRIPTION_ID = os.getenv("subscription_id")

RESOURCE_GROUP = "RG-SpotifyAzureProject"
FACTORY_NAME = "adfspotifyazureprj"
PIPELINE_NAME = "github_analyzer_daily_pipeline"


def get_databricks_run_id(
        pipeline_run_id,
        access_token
    ):

    AZURE_TENANT_ID = os.getenv("tenant_id")
    AZURE_CLIENT_ID = os.getenv("client_id")
    AZURE_CLIENT_SECRET = os.getenv("client_secret")
    AZURE_SUBSCRIPTION_ID = os.getenv("subscription_id")

    RESOURCE_GROUP = "RG-SpotifyAzureProject"
    FACTORY_NAME = "adfspotifyazureprj"
    PIPELINE_NAME = "github_analyzer_daily_pipeline"

    url = f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/microsoft.DataFactory/factories/{FACTORY_NAME}/pipelineruns/{pipeline_run_id}/queryActivityruns?api-version=2018-06-01"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # 🔥 Dynamic time window (last 1 hour)
    now = datetime.datetime.now()
    # body = {
    #     "lastUpdatedAfter": (now - timedelta(hours=1)).isoformat() + "Z",
    #     "lastUpdatedBefore": now.isoformat() + "Z"
    #     # "lastUpdatedAfter": "2026-04-18T00:00:00Z",
    #     # "lastUpdatedBefore": "2026-04-18T23:00:00Z"
        
    # }

    # ------------------------------------
    now = datetime.datetime.now(timezone.utc)

    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now.replace(hour=23, minute=59, second=59, microsecond=999000)

    body = {
        "lastUpdatedAfter": start_of_day.isoformat().replace("+00:00", "Z"),
        "lastUpdatedBefore": end_of_day.isoformat().replace("+00:00", "Z")
    }
    # ------------------------------------


    response = requests.post(url, headers=headers, json=body)


    data = response.json()

    db_run_id = None

    for item in data['value']:
        if item.get('output', {}).get('name') == 'db_run_id':
            db_run_id = item['output']['value']
            break

    # DEBUGGING start
    # st.write(db_run_id)

    # st.write("response we are getting for databricks run id")
    # st.write(body)
    # st.write(response.status_code)
    # st.write(response.json())
    # st.write(response.json()['value'][26]['output'])# ['value'])

    # DEBUGGING end



    if response.status_code != 200:
        print("Error:", response.text)
        return None

    return db_run_id

    # DEBUGGGING - You can safely remove this , its of no use 
    # activities = response.json().get("value", [])

    # # 🔍 Extract Databricks run_id
    # for activity in activities:
    #     if (
    #         activity.get("pipelineRunId") == pipeline_run_id and
    #         activity.get("activityName") == "Run_Databricks_Job"
    #     ):
    #         return activity.get("output", {}).get("db_run_id")

    # return None




import requests
import time
import streamlit as st

def poll_adf_pipeline_status(
    subscription_id,
    resource_group,
    factory_name,
    run_id,
    access_token
):
    url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{run_id}?api-version=2018-06-01"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    status = "InProgress"

    with st.spinner("ADF Pipeline is running..."):
        adf_status_placeholder = st.empty()
        while True:
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                st.error(f"Error fetching status: {response.text}")
                return None

            data = response.json()
            status = data.get("status")

            # Optional: get start/end time
            run_start = data.get("runStart")
            run_end = data.get("runEnd")

            # st.write(f"Current Status: {status}")
            # status_placeholder.write(f"Current Status: {status}")
            adf_status_placeholder.success(f"ADF Pipeline Status: {status}")

            if status in ["Succeeded", "Failed", "Cancelled"]:
                adf_status_placeholder.success(f"ADF Pipeline Finished: {status}")

                # Show timings - DEBUGGING
                # st.caption(f"Start Time: {run_start}")
                # st.caption(f"End Time: {run_end}")

                return status

            time.sleep(10)  # wait before next poll


def poll_databricks_pipeline_status(DATABRICKS_INSTANCE, DB_HEADERS, db_run_id):
    status = "PENDING"

    # life_cycle = None # DEBUGGING

    with st.spinner("Processing data..."):
        status_placeholder = st.empty()
        while True:
            status_response = requests.get(
                f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/get",
                headers=DB_HEADERS,
                params={"run_id": db_run_id}
            )

            state = status_response.json()["state"]
            life_cycle = state["life_cycle_state"]
            # st.write(state)
            
            if life_cycle not in ["TERMINATED", "SKIPPED", "FAILED"]:
                status_placeholder.success(f"Databricks Job Status: {life_cycle}")

            if life_cycle in ["TERMINATED", "SKIPPED", "FAILED"]:
                result_state = state.get("result_state", "UNKNOWN")
                # st.write(f"Job Finished: {result_state}")
                status_placeholder.success(f"Databricks Job Finished: {result_state}")
                break

            time.sleep(10)



from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "datalakespotifyazureprj"

# DONE : You needed to assign the role to the app or service principal in order to access the data in the storage account [INTERVIEW]
# Assigned following two roles from Access Control (IAM) in datalake storage account :
# ✔ Storage Blob Data Contributor (recommended)
# ✔ Storage Blob Data Reader (read-only)

def check_adls_data_exists(container_name, directory_path, tenant_id, client_id, client_secret):

    credential = ClientSecretCredential(
        tenant_id,
        client_id,
        client_secret
    )

    account_name = "datalakespotifyazureprj"

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    file_system_client = service_client.get_file_system_client(container_name)
    directory_client = file_system_client.get_directory_client(directory_path)


    try:
        paths = directory_client.get_paths(max_results=1)
        return any(True for _ in paths)
    except:
        return False
    


from databricks import sql


def read_data_from_adls(date, table, extra_conditions="", date_col='event_date', schema='gold'):

    DATABRICKS_INSTANCE = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    conn = sql.connect(
        server_hostname = DATABRICKS_INSTANCE,
        http_path = "/sql/1.0/warehouses/bb5b1b677264e4bd",
        access_token = DATABRICKS_TOKEN
    )

    query = f"""
        SELECT * 
        FROM github_cata.{schema}.{table} 
        WHERE DATE({date_col}) = '{date}' 
        {extra_conditions}
    """

    df = pd.read_sql(query, conn)

    return df



def extract_data_from_adls_for_query(query):
    DATABRICKS_INSTANCE = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    conn = sql.connect(
        server_hostname = DATABRICKS_INSTANCE,
        http_path = "/sql/1.0/warehouses/bb5b1b677264e4bd",
        access_token = DATABRICKS_TOKEN
    )

    df = pd.read_sql(query, conn)

    return df



def get_active_runs():
    DATABRICKS_INSTANCE = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/runs/list"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }
    
    params = {
        "active_only": "true"
    }

    response = requests.get(url, headers=headers, params=params)
    return response.json()

def is_pipeline_running_for_date(runs_json, target_date):
    for run in runs_json.get("runs", []):
        params = run.get("overriding_parameters", {}).get("notebook_params", {})
        
        if params.get("date") == target_date:
            return True
    
    return False

















# -------------------------
# CHART or VISUAL Functions
# -------------------------

# IN-USE
def render_normal_chart(df, x_col, y_col):

    df = df.sort_values(x_col)

    import plotly.express as px

    # fig = px.line(  # For Line Chart
    fig = px.bar(     # For Bar Chart
        df,
        x=x_col,
        y=y_col
    )

    fig.update_xaxes(
        title="Event Time",
        # showgrid=True,
        # gridcolor="gray",
        tickangle=0,
        # showline=True,
        # linewidth=2,
        # linecolor="white",
        # mirror=True
    )

    fig.update_yaxes(
        title="Event Count",
        # showgrid=True,
        # zeroline=True,
        # zerolinecolor="red"
    )
    st.plotly_chart(fig)


# IN-USE
def show_peak_hours(df, x_col, y_col):
    # top3 = df.sort_values(by="event_count", ascending=False).head(3)
    top3 = df.sort_values(by=y_col, ascending=False).head(3)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="🥇 Peak Hour",
            value=f"{top3.iloc[0][x_col]}:00",
            delta=f"{top3.iloc[0][y_col]} events"
        )

    with col2:
        st.metric(
            label="🥈 2nd Peak Hour",
            value=f"{top3.iloc[1][x_col]}:00",
            delta=f"{top3.iloc[1][y_col]} events"
        )

    with col3:
        st.metric(
            label="🥉 3rd Peak Hour",
            value=f"{top3.iloc[2][x_col]}:00",
            delta=f"{top3.iloc[2][y_col]} events"
        )


# IN-USE
def render_pivoted_chart(df, legend_title="Variables"):
    col_list = list(df.columns)
    df = df.sort_values(col_list[0])

    import plotly.express as px

    # fig = px.bar(     # For Bar Chart
    fig = px.line(  # For Line Chart
        df,
        x=col_list[0],
        y=col_list[1:] # selecting the columns from 1 and not from 0, (i.e. excluding the first column which is time_h) and going till last 2nd column (i.e. excluding the last column which is source_file column[added by pandas or may be by Streamlit])
    )

    fig.update_xaxes(
        title="Time",
        # showgrid=True,
        # gridcolor="gray",
        tickangle=0,
        # showline=True,
        # linewidth=2,
        # linecolor="white",
        # mirror=True
    )

    fig.update_yaxes(
        title="Event Count",
        # showgrid=True,
        # zeroline=True,
        # zerolinecolor="red"
    )

    fig.update_layout(
        legend_title_text=(f"<b>{legend_title}</b>"),
    )
    st.plotly_chart(fig)


# IN-USE
def show_top_3_starred_repo(df):
    top_repos_df = (
        df.sort_values("no_of_watchers", ascending=False)
        .head(3)
        .copy()
    )

    top_repos = top_repos_df.to_dict(orient="records")

    st.markdown("""
    <style>
    .repo-card {
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 16px;
        background-color: #0d1117;
        color: #c9d1d9;
        transition: 0.2s;
    }
    .repo-card:hover {
        border-color: #58a6ff;
        transform: translateY(-3px);
    }
    .repo-title {
        font-size: 18px;
        font-weight: 600;
        color: #58a6ff;
    }
    .repo-desc {
        font-size: 14px;
        margin: 8px 0;
        color: #8b949e;
    }
    .repo-footer {
        font-size: 13px;
        display: flex;
        justify-content: space-between;
    }
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns(3)

    for i, repo in enumerate(top_repos):
        with cols[i]:
            html_part = f"""
            <div class="repo-card">
                <div class="repo-title">#{i+1} {repo['repo_name']}</div>
                <div class="repo-desc">As of Date : {repo['event_date']}</div>
                <div class="repo-footer">
                    <span>👀 {repo['no_of_watchers']}</span>
                </div>
            </div>
            <br>
            """

            st.markdown(html_part, unsafe_allow_html=True)


# IN-USE
def show_top_3_repo_by_pushes(df):
    top_repos_df = (
        df.sort_values("no_of_pushes", ascending=False)
        .head(3)
        .copy()
    )

    top_repos = top_repos_df.to_dict(orient="records")

    st.markdown("""
    <style>
    .repo-card {
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 16px;
        background-color: #0d1117;
        color: #c9d1d9;
        transition: 0.2s;
    }
    .repo-card:hover {
        border-color: #58a6ff;
        transform: translateY(-3px);
    }
    .repo-title {
        font-size: 18px;
        font-weight: 600;
        color: #58a6ff;
    }
    .repo-desc {
        font-size: 14px;
        margin: 8px 0;
        color: #8b949e;
    }
    .repo-footer {
        font-size: 13px;
        display: flex;
        justify-content: space-between;
    }
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns(3)

    for i, repo in enumerate(top_repos):
        with cols[i]:
            html_part = f"""
            <div class="repo-card">
                <div class="repo-title">#{i+1} {repo['repo_name']}</div>
                <div class="repo-desc">As of Date : {repo['event_date']}</div>
                <div class="repo-footer">
                    <span style="color:#3fb950;">▲ {repo['no_of_pushes']} events</span>
                </div>
            </div>
            <br>
            """
            
            st.markdown(html_part, unsafe_allow_html=True)


# IN-USE
def display_top_users(df):
    # Safety check
    required_cols = [
        "username",
        "no_of_Events_per_user",
        "event_date",
        "rank_on_that_day"
    ]
    
    if not all(col in df.columns for col in required_cols):
        st.error("DataFrame missing required columns")
        return

    # Sort by rank (important if not already sorted)
    df = df.sort_values("rank_on_that_day").head(5)

    # Title + context
    event_date = df["event_date"].iloc[0]
    st.caption("Number of events per user")

    # Create columns
    cols = st.columns(len(df))

    for i, col in enumerate(cols):
        row = df.iloc[i]

        # Medal icons for top 3
        medal = ["🥇", "🥈", "🥉"]
        prefix = medal[i] if i < 3 else f"#{i+1}"

        col.metric(
            label=f"{prefix} {row['username']}",
            value=row["no_of_Events_per_user"],
            delta="Events"  # 👈 this clarifies meaning
        )


# IN-USE
import plotly.graph_objects as go

def display_daily_github_metrics(df):
    """
    Expects df with exactly ONE row
    """

    if df.empty:
        st.warning("No data available")
        return

    row = df.iloc[0]

    # -----------------------------
    # 🧠 HEADER
    # -----------------------------
    # st.title("📊 GitHub Daily Activity Dashboard")
    # st.caption("Insights displayed below are for a particular day.")
    st.markdown("""
        <h1 style='margin-bottom: 0;'>📊GitHub Daily Activity Dashboard</h1>
        <p style='margin-top: 0; color: gray;'>
        Insights displayed below are for a particular day.
        </p>
    """, unsafe_allow_html=True)
    
    st.caption(f"📅 Date: {row['event_date']}")

    # -----------------------------
    # 🔹 KPI SECTION
    # -----------------------------
    st.markdown("### 🔑 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("👥 Users", row["no_of_unique_users"])
    col2.metric("📦 Events", row["total_events"])
    col3.metric("🔀 PR Merge Rate (Day)", f"{row['pr_merge_rate']:.2%}")
    col4.metric("⚖️ Issue/PR Ratio (Day)", f"{row['issue_pr_activity_ratio_daily']:.2f}")

    st.divider()

    # -----------------------------
    # 🔹 CONTRIBUTION BREAKDOWN
    # -----------------------------
    st.markdown("### 🔄 Contribution Breakdown")

    fig = go.Figure(data=[
        go.Bar(name="Count", x=[
            "Issues Opened",
            "PRs Created",
            "PRs Merged",
            "PRs Closed"
        ],
        y=[
            row["issues_opened"],
            row["prs_created"],
            row["prs_merged"],
            row["prs_closed"]
        ])
    ])

    fig.update_layout(title="PR & Issue Activity")
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # -----------------------------
    # 🔹 REPO DISTRIBUTION
    # -----------------------------
    st.markdown("### 📂 Repository Distribution")

    repo_fig = go.Figure(data=[go.Pie(
        labels=["Public Repos", "Private Repos"],
        values=[row["total_public_repo"], row["total_private_repo"]]
    )])

    st.plotly_chart(repo_fig, use_container_width=True)

    # -----------------------------
    # 🔹 USER ENGAGEMENT
    # -----------------------------
    st.markdown("### 🧑‍💻 Engagement Insights")

    col1, col2, col3 = st.columns(3)

    col1.metric("Repos Modified", row["no_of_unique_repos_modified"])
    col2.metric("Organizations", row["no_of_unique_organizations"])
    col3.metric("Avg Events/User", 
                round(row["total_events"] / row["no_of_unique_users"], 2)
                if row["no_of_unique_users"] > 0 else 0)

    # -----------------------------
    # 🔹 EFFICIENCY INDICATORS
    # -----------------------------
    st.markdown("### ⚡ Efficiency Indicators")

    efficiency_col1, efficiency_col2 = st.columns(2)

    # Gauge for PR merge rate
    gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=row["pr_merge_rate"],
        title={"text": "PR to Merge Rate (for the day)"},
        gauge={'axis': {'range': [0, 1]}}
    ))

    efficiency_col1.plotly_chart(gauge, use_container_width=True)

    # Issue/PR ratio bar
    ratio_fig = go.Figure(go.Bar(
        x=["Issue/PR Ratio (for the day)"],
        y=[row["issue_pr_activity_ratio_daily"]]
    ))

    efficiency_col2.plotly_chart(ratio_fig, use_container_width=True)

    # # -----------------------------
    # # 🔹 RAW DATA
    # # -----------------------------
    # st.markdown("### 📋 Raw Data")
    # st.dataframe(df)












# NOT-IN-USE
def show_private_repos():
    import streamlit as st

    private_repos = [
        {"name": "Internal-API", "updated": "2 days ago", "owner": "Data Team", "branch": "main", "status": "Active"},
        {"name": "ETL-Pipeline", "updated": "5 days ago", "owner": "Backend Team", "branch": "dev", "status": "Active"},
        {"name": "Client-Dashboard", "updated": "10 days ago", "owner": "Frontend Team", "branch": "main", "status": "Archived"},
    ]

    cols = st.columns(3)

    for i, repo in enumerate(private_repos):
        with cols[i]:
            with st.container(border=True):
                st.markdown(f"### 🔒 {repo['name']}")
                st.caption("PRIVATE")

                st.write(f"👤 **Owner:** {repo['owner']}")
                st.write(f"🌿 **Branch:** `{repo['branch']}`")
                st.write(f"🕒 **Updated:** {repo['updated']}")

                if repo["status"] == "Active":
                    st.success("🟢 Active")
                else:
                    st.warning("⚪ Archived")


# NOT-IN-USE
def show_table(df):
    column_config = {
        col: st.column_config.Column(label=col.replace("_", " ").title())
        for col in df.columns
    }

    st.dataframe(
        df,
        column_config=column_config,
        use_container_width=True,
        hide_index=True
    )
    # from st_aggrid import AgGrid, GridOptionsBuilder

    # gb = GridOptionsBuilder.from_dataframe(df)
    # gb.configure_pagination()
    # gb.configure_side_bar()
    # gb.configure_default_column(editable=True, filter=True, sortable=True)

    # grid_options = gb.build()

    # AgGrid(
    #     df,
    #     gridOptions=grid_options,
    #     fit_columns_on_grid_load=True,
    # )











