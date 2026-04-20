# Databricks notebook source
# Enables autoreload;
%load_ext autoreload
%autoreload 2

import os
import sys
import uuid

project_path = os.path.join(os.getcwd(), '..','..')
sys.path.append(project_path)

import utils as utils

from pyspark.sql.functions import * 
from pyspark.sql.types import *
from datetime import datetime

# dbutils.widgets.text("date",datetime.now().strftime("%Y-%m-%d"))
dbutils.widgets.text("date","2015-01-01")
dbutils.widgets.text("time_h", "1")
dbutils.widgets.text("duration", "1")

date = dbutils.widgets.get("date")
time_h = dbutils.widgets.get("time_h")
duration = dbutils.widgets.get("duration")


dt = datetime.strptime(date, "%Y-%m-%d")
p_year = dt.year
p_month = dt.month
p_day = dt.day

print(f"Received input: {date, time_h, duration, p_year, p_month, p_day}")

# COMMAND ----------

# MAGIC %md
# MAGIC **READING DATA for only PROVIDED DATE**

# COMMAND ----------

table = "github_cata.silver.silver_raw"
query = f"""
    SELECT * FROM {table}
    WHERE DATE(created_at) = "{date}"
"""
silver_raw_df = spark.sql(query)
# silver_raw_df.display()

# COMMAND ----------

silver_raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Event Hour Distribution**

# COMMAND ----------

event_hour_df = silver_raw_df.groupBy('type', 'created_at').agg(count('*').alias('no_of_events'))

pivot_event_hour_df = (
    event_hour_df.groupBy(hour('created_at').alias('hour'))
                .pivot('type')
                .agg(sum('no_of_events').alias('no_of_events'))
                .orderBy('hour')
)
pivot_event_hour_df = pivot_event_hour_df.withColumn('event_date', lit(date))

# COMMAND ----------

utils.write_delta_partitioned(
    df = pivot_event_hour_df,
    schema = "gold",
    table_name = "event_hour_distribution"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Total Events per Hour**

# COMMAND ----------

total_events_per_hour_df = (
    silver_raw_df.withColumn('hour', hour(col('created_at')))
)


total_events_per_hour_df = (
    total_events_per_hour_df.groupBy('hour')
                            .agg(count('*').alias('no_of_events'))
                            .orderBy('hour')
)

total_events_per_hour_df = (
    total_events_per_hour_df.withColumn('event_date', lit(date))
)

# total_events_per_hour_df.display()

# COMMAND ----------

utils.write_delta_partitioned(
    df = total_events_per_hour_df,
    schema = "gold",
    table_name = "total_events_per_hour"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Count of Different Events throught the day**

# COMMAND ----------

total_diff_events_df = (
    silver_raw_df.groupBy('type').agg(count('*').alias('no_of_events'))
)
total_diff_events_df = total_diff_events_df.withColumn('event_date', lit(date))

# COMMAND ----------

utils.write_delta_partitioned(
    df = total_diff_events_df,
    schema = "gold",
    table_name = "total_diff_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Aggregated Table**

# COMMAND ----------

# countDistinct() ignores Null, i.e. it doesn't counts them

# NOTE : Here we're only defining the metrics, but we would be calculating this in 'Smart behavioral approximation' Cell. The final aggregation is done there

metrics_1 = [
    countDistinct('username').alias('no_of_unique_users'),
    countDistinct('repo').alias('no_of_unique_repos_modified'),
    countDistinct(when(col("public") == True, col('repo'))).alias("total_public_repo"),
    countDistinct(when(col("public") == False, col('repo'))).alias("total_private_repo"),
    countDistinct(('org_name')).alias('no_of_unique_organizations'),
    count('*').alias('total_events')
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Top Real Users**

# COMMAND ----------

from pyspark.sql.window import Window

top_10_real_users_df = silver_raw_df.groupBy('username')\
                                    .agg(count('*').alias('no_of_Events_per_user'))\
                                    .orderBy(col('no_of_Events_per_user').desc())

top_10_real_users_df = top_10_real_users_df.filter(
    ~(col('username').contains('[bot]')) &
    ~(col('username').endswith('-bot')) &
    (col('username') != "Copilot")
).limit(10)

top_10_real_users_df = top_10_real_users_df.withColumn('event_date', lit(date))

window_spec = Window.orderBy(col('no_of_events_per_user').desc())
top_10_real_users_df = (
    top_10_real_users_df
        .withColumn('rank_on_that_day', dense_rank().over(window_spec))
)
# top_10_real_users_df.display()

# COMMAND ----------

utils.write_delta_partitioned(
    df = top_10_real_users_df,
    schema = "gold",
    table_name = "top_10_real_users"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Trending/Top Repo per Hour**

# COMMAND ----------

from pyspark.sql.window import Window
window_spec = Window.partitionBy(col('hour')).orderBy(col('no_of_events').desc())
top_repo_per_hour_df = silver_raw_df.withColumn('hour', date_trunc('hour', col('created_at')))\
                                .groupBy(col('hour'), col('repo_name'))\
                                .agg(count('*').alias('no_of_events'))\
                                .withColumn('rank', dense_rank().over(window_spec))\
                                .filter(col('rank')==1)\
                                .drop('rank')

# top_repo_per_hour_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **TOP 3 Repo's throughout the Day**
# MAGIC - Here we are capturing the Behaviour of Top 3 Repos of the day thorought the day. Defining Top 3 by no_of_events related to that repo on that particular day

# COMMAND ----------

n_top_repo = 3 # select how many number of repos do you want 
top_3_repo_df = silver_raw_df.groupBy(col('repo_name')).agg(count("*").alias('no_of_events')).orderBy(col('no_of_events').desc()).limit(n_top_repo)

top_3_repo_list = [row.repo_name for row in top_3_repo_df.select('repo_name').collect()] 

print((top_3_repo_list))

top_3_repo_trend_df = silver_raw_df.filter(col('repo_name').isin(top_3_repo_list))\
                                .withColumn('hour', date_trunc('hour', col('created_at')))\
                                .groupBy(col('repo_name'), col('hour'))\
                                .agg(count('*').alias('no_of_events'))
                                # You can change the order of col's in the groupBy, it will result in the same output, only order changed

pivoted_top_3_repo_trend_df = top_3_repo_trend_df.groupBy(col('hour'))\
                                                    .pivot('repo_name')\
                                                    .agg(sum('no_of_events'))\
                                                    .orderBy('hour')

pivoted_top_3_repo_trend_df = pivoted_top_3_repo_trend_df.withColumn('event_date', lit(date))
# pivoted_top_3_repo_trend_df.display()

# COMMAND ----------

utils.write_delta_partitioned(
    df = pivoted_top_3_repo_trend_df,
    schema = "gold",
    table_name = "top_3_repo_trend"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Top 5 Orgs Trend throught the day**

# COMMAND ----------

# Finding top n orgs
n_top_orgs = 5
top_orgs = silver_raw_df.filter(col('org_name').isNotNull()).groupBy(col('org_name')).agg(count('*').alias('no_of_events')).orderBy(col('no_of_events').desc()).limit(n_top_orgs)

top_n_orgs_list = [row.org_name for row in top_orgs.select('org_name').collect()]

# Calculating hourly events occured for top n orgs
top_n_orgs_trend_df = silver_raw_df.filter(col('org_name').isin(top_n_orgs_list))\
                                .withColumn('hour', date_trunc('hour', col('created_at')))\
                                .groupBy(col('org_name'), col('hour'))\
                                .agg(count('*').alias('no_of_events'))
                                # You can change the order of col's in the groupBy, it will result in the same output, only order changed

top_n_orgs_trend_df = top_n_orgs_trend_df.withColumnRenamed('org_name', 'Organization Name')

# Creating the Pivoted DF
pivoted_top_n_orgs_trend_df = top_n_orgs_trend_df.groupBy(col('hour'))\
                                                    .pivot('Organization Name')\
                                                    .agg(sum('no_of_events'))\
                                                    .orderBy('hour')
pivoted_top_n_orgs_trend_df = pivoted_top_n_orgs_trend_df.withColumn('event_date', lit(date))

# COMMAND ----------

utils.write_delta_partitioned(
    df = pivoted_top_n_orgs_trend_df,
    schema = "gold",
    table_name = "top_3_org_trend"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **🚀 Repo Popularity (WatchEvent)**

# COMMAND ----------

# WatchEvent = ⭐ Star event
df_watch = silver_raw_df.filter(col('type') == "WatchEvent")

top_5_starred_repo_df = df_watch.groupBy("repo").agg(count('*').alias('no_of_watchers')).orderBy(col('no_of_watchers').desc()).limit(5)

top_5_starred_repo_df = top_5_starred_repo_df.select(
    col('repo').alias('repo'),
    split(col("repo"), "/")[0].alias("repo_owner"),
    split(col("repo"), "/")[1].alias("repo_name"),
    col('no_of_watchers')
)

# top_5_starred_repo_list = [row.repo for row in top_5_starred_repo_df.select('repo').collect()]

top_5_starred_repo_df = top_5_starred_repo_df.withColumn('event_date', lit(date))

# COMMAND ----------

utils.write_delta_partitioned(
    df = top_5_starred_repo_df,
    schema = "gold",
    table_name = "most_starred_repos"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Top active repos (by pushes)**

# COMMAND ----------

df_push = silver_raw_df.filter(col('type') == "PushEvent")

top_3_repos_by_pushes_df = df_push.groupBy("repo_name").agg(count('*').alias('no_of_pushes')).orderBy(col('no_of_pushes').desc()).limit(3)

top_3_repos_by_pushes_df = top_3_repos_by_pushes_df.withColumn('event_date', lit(date))
# top_3_repos_by_pushes_df.display()

# COMMAND ----------

utils.write_delta_partitioned(
    df = top_3_repos_by_pushes_df,
    schema = "gold",
    table_name = "top_3_repos_by_pushes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **✅ Smart behavioral approximation**
# MAGIC - **Issues activity → PR activity → PR completion**
# MAGIC - **NOTE :** - All the below figures are for that particular day , so you can call it like -> (issues_opened today, ..)
# MAGIC - **industry standard**

# COMMAND ----------

# # [ORIGINAL]
# metrics_2 = [
#     countDistinct(
#         when(
#             (col("type") == "IssuesEvent") & (col("payload.action") == "opened"),
#             col("payload.issue.id")
#         )
#     ).alias("issues_opened"),

#     countDistinct(
#         when(
#             (col("type") == "PullRequestEvent") & (col("payload.action") == "opened"),
#             col("payload.pull_request.id")
#         )
#     ).alias("prs_created"),

#     countDistinct(
#         when(
#             (col("type") == "PullRequestEvent") & (col("payload.action") == "merged"),
#             col("payload.pull_request.id")
#         )
#     ).alias("prs_merged"),

#     countDistinct(
#         when(
#             (col("type") == "PullRequestEvent") & (col("payload.action") == "closed"),
#             col("payload.pull_request.id")
#         )
#     ).alias("prs_closed")
# ]

# # NOTE : Here we're doing the final aggregation. And the metrics_1 are defined above in cell 'Aggregated Table'.
# aggregated_df = silver_raw_df.agg(*metrics_1, *metrics_2)

# aggregated_df = aggregated_df.withColumn('event_date', lit(date))

# COMMAND ----------

# [USING THIS] - As you are now not storing the payload field , therefore directly using the id's stored
metrics_2 = [
    countDistinct(
        when(
            (col("type") == "IssuesEvent") & (col("action") == "opened"),
            col("issue_id")
        )
    ).alias("issues_opened"),

    countDistinct(
        when(
            (col("type") == "PullRequestEvent") & (col("action") == "opened"),
            col("pr_id")
        )
    ).alias("prs_created"),

    countDistinct(
        when(
            (col("type") == "PullRequestEvent") & (col("action") == "merged"),
            col("pr_id")
        )
    ).alias("prs_merged"),

    countDistinct(
        when(
            (col("type") == "PullRequestEvent") & (col("action") == "closed"),
            col("pr_id")
        )
    ).alias("prs_closed")
]

# NOTE : Here we're doing the final aggregation. And the metrics_1 are defined above in cell 'Aggregated Table'.
aggregated_df = silver_raw_df.agg(*metrics_1, *metrics_2)

aggregated_df = aggregated_df.withColumn('event_date', lit(date))

# COMMAND ----------

# First Check & Read it. For My Knowledge. Can be helpful in INTERVIEW
# For a given day, how many PRs are being created relative to issues being opened.
# Issue-to-PR Activity Ratio
# issue_to_pr_activity_ratio = issues_opened / prs_created
# print(issue_to_pr_activity_ratio)
# 👉 Interpretation:
# 🔹 > 1
# 👉 More PRs than issues
# ✔ Team is clearing backlog / very productive

# 🔹 ≈ 1
# ✔ Balanced system
# 👉 Work coming in ≈ work being processed

# 🔹 < 1
# 👉 More issues than PRs
# ⚠ Backlog is growing

# PR Merge Rate
# pr_merge_rate = prs_merged / prs_created
# print(pr_merge_rate)
# 🔹 High (80–100%)
# 🔹 Medium (50–80%)
# 🔹 Low (<50%)

# 👉 PR/Issue Ratio: “Measures how effectively the team is handling incoming work.”
# 👉 PR Merge Rate: “Measures the success rate and quality of code contributions.”

# Point for INTERVIEW
# Since GH Archive doesn’t provide reliable issue-to-PR linkage for a single day, I don’t treat it as a true conversion metric. Instead, I model it as an activity ratio — comparing PR creation volume to issue creation volume — while keeping PR-to-merge as a true conversion metric.”

# COMMAND ----------

aggregated_df = aggregated_df.withColumn(
    "issue_pr_activity_ratio_daily",
    round(
        when(col("prs_created") != 0,
             (col("issues_opened") / col("prs_created")) * 100
        ).otherwise(0),
        2
    )
).withColumn(
    "pr_merge_rate",
    round(
        when(col("prs_created") != 0,
             (col("prs_merged") / col("prs_created")) * 100
        ).otherwise(0),
        2
    )
).withColumn(
    "pr_merge_rate",
    round(
        when(col("prs_created") != 0,
             (col("prs_closed") / col("prs_created")) * 100
        ).otherwise(0),
        2
    )
)

# Number of issues opened vs PRs created on the same day (independent events)
# **Note:** Issue-to-PR ratio compares the number of issues opened and pull requests created on the same day. These are independent activities and do not represent a direct conversion or relationship between issues and PRs.

# aggregated_df.display()

# COMMAND ----------

utils.write_delta_partitioned(
    df = aggregated_df,
    schema = "gold",
    table_name = "overview_table"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Repo Activity Score**
# MAGIC ### **Daily PR Activity Breakdown**
# MAGIC ##### **NOTE** : You can include the above two calculations as well from Github_analyzer_v0