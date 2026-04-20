# Databricks notebook source
# Enables autoreload;
%load_ext autoreload
%autoreload 2

import os
import sys
import uuid

project_path = os.path.join(os.getcwd())
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

# date = '2026-04-01'
layer = "bronze"
path = f"abfss://githubanalyzer@datalakespotifyazureprj.dfs.core.windows.net/{layer}/{date}"

raw_df = spark.read.format('json')\
            .option('inferSchema', True)\
            .option("multiLine", False)\
            .load(path)
# Adding 'file_path' value 
raw_df = raw_df.withColumn("source_file", col("_metadata.file_path"))

# COMMAND ----------

# raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **CLEANING THE DATA**

# COMMAND ----------

def null_check(df, col_name):
    return df.filter(col(f"{col_name}").isNotNull())

not_null_list = ["id", "type", "actor.login", "repo.name", "payload"]

df_clean = raw_df.dropDuplicates(["id"])

for column in not_null_list:
    df_clean = null_check(df_clean, column)

# COMMAND ----------

# MAGIC %md
# MAGIC **FLATTENING**

# COMMAND ----------

df_clean = df_clean.withColumn("repo_owner", split(col('repo.name'), '/')[0])\
                    .withColumn("repo_name", split(col('repo.name'), '/')[1])
df_clean = df_clean.withColumn("year",year("created_at"))\
                    .withColumn("month",month("created_at"))\
                    .withColumn("day",day("created_at"))

# the ones commented out can be extracted as needed 
df_flat = df_clean.select(

    # --- top level ---
    col("id").alias("event_id"),
    col("type"),
    col("created_at"),
    col("public"),

    # --- actor ---
    col("actor.id").alias("user_id"),
    col("actor.login").alias("username"),
    col("actor.url").alias("actor_url"),

    # --- repo ---
    col("repo.id").alias("repo_id"),
    col("repo.name").alias("repo"),
    col("repo_owner"),
    col("repo_name"),
    col("repo.url").alias("repo_url"),

    # --- org (optional but useful) ---
    col("org.id").alias("org_id"),
    col("org.login").alias("org_name"),

    # --- payload (important fields) ---
    col("payload.action").alias("action"),
    # col("payload.ref").alias("ref"),
    # col("payload.ref_type").alias("ref_type"),
    # col("payload.head").alias("head"),
    # col("payload.before").alias("before"),
    # col("payload.size").alias("commit_count"),

    # --- payload: pull request ---
    col("payload.pull_request.id").alias("pr_id"),
    # col("payload.pull_request.number").alias("pr_number"),
    # col("payload.pull_request.state").alias("pr_state"),
    # col("payload.pull_request.title").alias("pr_title"),
    # col("payload.pull_request.user.login").alias("pr_user"),
    # col("payload.pull_request.merged").alias("pr_merged"),

    # --- payload: issue ---
    col("payload.issue.id").alias("issue_id"),
    # col("payload.issue.number").alias("issue_number"),
    # col("payload.issue.title").alias("issue_title"),
    # col("payload.issue.state").alias("issue_state"),
    # col("payload.issue.user.login").alias("issue_user"),

    # --- payload: comment ---
    col("payload.comment.id").alias("comment_id"),
    col("payload.comment.body").alias("comment_body"),
    col("payload.comment.user.login").alias("comment_user"),

    # --- keep RAW payload ---
    # col("payload").alias("payload"),
    
    col("source_file"),
    col("year"),
    col("month"),
    col("day"),
)

# df_flat.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **WRITING DATA to TABLE**

# COMMAND ----------

utils.write_delta_partitioned(
    df = df_flat,
    schema = "silver",
    table_name = "silver_raw",
    date_col = "created_at"
)

# COMMAND ----------

# %sql
# select * from github_cata.gold.overview_table where event_date = '2015-01-05'