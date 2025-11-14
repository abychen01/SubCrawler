# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0bcf40e1-5936-4fdc-af5b-c02a4546065b",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "2a8af919-0041-46ee-b6c9-e0fcee3bb1c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "0bcf40e1-5936-4fdc-af5b-c02a4546065b"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "748aae2a-cba8-8010-4e4a-ee7a1d06a587",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%pip install google-auth-oauthlib google-api-python-client
pip install praw 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Imports

# CELL ********************

#library google-auth-oauthlib , google-auth-oauthlib, praw installed in custom-env

from notebookutils import mssparkutils
#from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, desc
import praw, io, json, time
from delta.tables import DeltaTable
from google.oauth2 import service_account



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Schema def

# CELL ********************

schema_sub = StructType(
    [
        StructField("subreddits",StringType())
    ]
)
schema_key = StructType(
    [
        StructField("keywords",StringType())
    ]
)

schema_submissions = StructType([
    StructField("subreddit_name", StringType(), True),
    StructField("key", StringType(), True),
    StructField("post_title", StringType(), True),
    StructField("id", StringType(), True),
    StructField("PermaLink", StringType(), True),
    StructField("Comments", IntegerType(), True),
    StructField("Self_text", StringType(), True),
    StructField("time_utc", DoubleType(), True),
    StructField("upvote_ratio", DoubleType(), True)

])

schema_comments = StructType([
    StructField("submission_id", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("comment_body", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("permalink", StringType(), True),
    StructField("time_utc", DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_reddit_creds = spark.read.format("delta").load("Files/creds")
df_reddit_creds = df_reddit_creds.collect()[0]

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "/tmp/service_account_creds.json"
DRIVE_FOLDER_ID = "1VNub-8hZYgBb7Jq4qlUZBUQ7h-dJWuty"  #  Drive folder ID

reddit = praw.Reddit(
    client_id = df_reddit_creds.reddit_id,
    client_secret = df_reddit_creds.reddit_secret,
    user_agent = df_reddit_creds.reddit_user_agent
)

b_submissions = "Bronze_LH.submissions"
b_comments_indirect = "Bronze_LH.comments_indirect"
b_comments_direct = "Bronze_LH.comments_direct"
s_comments = "Silver_LH.comments"
s_submissions = "Silver_LH.submissions"
g_comments = "Gold_LH.comments"
g_submissions = "Gold_LH.submissions"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# -----------------------------
# Step 1: Copy credentials to /tmp (skip if already done)
# -----------------------------
# Replace 'Lakehouse/Files/service_account_creds.json' with actual location
mssparkutils.fs.cp("Files/service_account_creds.json", "file:/tmp/service_account_creds.json", recurse=False)

# -----------------------------
# Step 2: Setup Google Drive service with Service Account
# -----------------------------
SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "/tmp/service_account_creds.json"

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# -----------------------------
# Step 3: Define flags (used to detect changes)
# -----------------------------
same_sub = False
same_key = False

# -----------------------------
# Step 4: Download Drive CSV → /tmp → Compare → Save to Lakehouse
# -----------------------------
def get_drive_data(file_id, file_name):
    global same_sub, same_key
    try:
        request = drive_service.files().export_media(fileId=file_id, mimeType='text/csv')
        local_tmp_path = f"/tmp/{file_name}.csv"

        with io.FileIO(local_tmp_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        # Check if file already exists in Lakehouse
        file_exists = False
        try:
            file_exists = True if mssparkutils.fs.ls(f"Files/tmp/{file_name}.csv") else False
        except:
            pass  # file doesn't exist

        if file_exists and file_name in ("subreddits", "keywords"):
            # Copy newly downloaded file to temp file for comparison
            mssparkutils.fs.cp(f"file:{local_tmp_path}", f"Files/tmp/{file_name}_temp.csv")

            # Load existing + temp files
            df = spark.read.option("header", False).csv(f"Files/tmp/{file_name}.csv")
            df_temp = spark.read.option("header", False).csv(f"Files/tmp/{file_name}_temp.csv")

            diff1 = df.exceptAll(df_temp)
            diff2 = df_temp.exceptAll(df)

            if diff1.isEmpty() and diff2.isEmpty():
                if file_name == "subreddits":
                    same_sub = True
                elif file_name == "keywords":
                    same_key = True
            else:
                # Replace old file with new one
                df_temp.write.format("csv").mode("overwrite").save(f"Files/tmp/{file_name}.csv")
                same_sub = False
                same_key = False

        else:
            # First-time save
            mssparkutils.fs.cp(f"file:{local_tmp_path}", f"Files/tmp/{file_name}.csv")

    except Exception as e:
        print(f"    Error in get_drive_data for {file_name}: {e}")

# -----------------------------
# Step 5: List all files in a folder and process target files
# -----------------------------
def list_files_in_folder(service, folder_id):
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, fields="files(id, name)", pageSize=1000).execute()
        files = results.get('files', [])

        if not files:
            print("No files found.")
        else:
            for file in files:
                if file['name'] in ("subreddits", "keywords", "email"):
                    get_drive_data(file['id'], file['name'])
                else:
                    print(f"Skipping: {file['name']}")
    except Exception as e:
        print(f"    Error in list_files_in_folder: {e}")


list_files_in_folder(drive_service, DRIVE_FOLDER_ID)

# -----------------------------
# Step 7: Final flags
# -----------------------------
print(f"same_sub: {same_sub}")
print(f"same_key: {same_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sub = spark.read.schema(schema_sub).csv("Files/tmp/subreddits.csv")
df_key = spark.read.schema(schema_key).csv("Files/tmp/keywords.csv")

sub_rows = df_sub.take(df_sub.count())
key_rows = df_key.take(df_key.count())

# testing ....
df_sub.show()
df_key.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


submission_list = []
table_exists = ""
both_same = True if same_key and same_sub else False
print(f"both_same={both_same}")#testing

def fn_test():
    try: 
            for index, srow in enumerate(sub_rows):
                for index, krow in enumerate(key_rows):
                    for submission in reddit.subreddit(srow.subreddits).new(limit=500):
                        if krow.keywords.lower() in submission.title.lower() or krow.keywords.lower() in submission.selftext.lower():
                            if submission.created_utc > 1735689600:
                                submission_list.append((srow.subreddits, krow.keywords, submission.title, submission.id, \
                                                    submission.permalink, submission.num_comments, submission.selftext,\
                                                    submission.created_utc, submission.upvote_ratio))
            df = spark.createDataFrame(submission_list, schema = schema_submissions)
            df.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(b_submissions)
            print("default and submissions - ")#testing
            display(df)#testing

    except Exception as e:
            print(f"    Error : {e}")

try:
    table_exists = True if mssparkutils.fs.ls("Tables/submissions") else False
    print(f"table_exists={table_exists}")#testing

except Exception as e:
    table_exists = False
    print(f"table_exists={table_exists}")#testing

if table_exists and both_same:

    df_sub_temp = spark.read.table("submissions")
    if df_sub_temp.count() > 0:

        latest_date = df_sub_temp.sort(desc(col("time_utc"))).select("time_utc").first()[0]
        print(f"latest_date={latest_date}")

        try: 
            for index, srow in enumerate(sub_rows):
                for index, krow in enumerate(key_rows):
                    for submission in reddit.subreddit(srow.subreddits).new(limit=500):
                        if krow.keywords.lower() in submission.title.lower() or krow.keywords.lower() in submission.selftext.lower():
                            if submission.created_utc > latest_date:
                                submission_list.append((srow.subreddits, krow.keywords, submission.title, submission.id, \
                                                    submission.permalink, submission.num_comments, submission.selftext,\
                                                    submission.created_utc, submission.upvote_ratio))

            df_update = spark.createDataFrame(submission_list, schema = schema_submissions)
            print("both exists and submissions_temp - ") #testing
    
            df_update.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable("submissions_temp")


            df_update = spark.read.table("submissions_temp")

            if df_update.count() > 0:
                
                display(df_update) #testing
                updates = True


                delta_table = DeltaTable.forName(spark, "submissions")

                source_key = "id"
                target_key = "id"

                columns = [col for col in df_update.columns if col != source_key]

                update_condition = " OR ".join([f"target.{col} != source.{col}" for col in columns])

                update_set = {col: f"source.{col}" for col in columns}

                insert = {target_key: f"source.{source_key}", **{col: f"source.{col}" for col in columns}}

                # Perform merge with error handling
                try:
                    merge = delta_table.alias("target")\
                        .merge(
                            source=df_update.alias("source"),
                            condition=f"target.{target_key} = source.{source_key}"
                        )\
                        .whenMatchedUpdate(
                            condition=update_condition,
                            set=update_set
                        )\
                        .whenNotMatchedInsert(
                            values=insert
                        )

                    merge_result = merge.execute()
                    print("Merge executed successfully")
                    
                    
                except Exception as e:
                    print(f"Merge failed: {e}")
            
            else:
                print("no new rows")
                mssparkutils.fs.rm("Tables/submissions_temp", recurse=True)
                print("submissions_temp table deleted")
                updates = False

            df2 = spark.read.table("submissions")
            print("final and submissions - ")
            display(df2)

        except Exception as e:
            print(f"    Error : {e}")
        
    else:
        
        fn_test()


if table_exists and both_same == False:

    mssparkutils.fs.rm("Tables/submissions", recurse=True)
    mssparkutils.fs.rm("Tables/comments_direct", recurse=True)
    mssparkutils.fs.rm("Tables/comments_indirect", recurse=True)

    print("table exist but deleted")
    updates = False

    fn_test()
    print("table created")

if table_exists == False:
    
    updates = False
    print("table doesn't exist")
    fn_test()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_submissions = spark.read.table(b_submissions)
srows = df_submissions.take(df_submissions.count())
comments_list = []

for index, srow in enumerate(srows):
    submission_id = srow.id
    submission = reddit.submission(id = submission_id)
    submission.comments.replace_more(limit=None)
    

    for comment in submission.comments.list():
        comments_list.append((submission_id, comment.id, "N/A", comment.body,\
                                 comment.score, comment.permalink, comment.created_utc))

df_comments = spark.createDataFrame(comments_list,schema = schema_comments)
df_comments.write.format("delta").mode("overwrite")\
                    .option("overwriteSchema", True).saveAsTable(b_comments_direct)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


max_posts_per_keyword = 100
df_sub = spark.read.schema(schema_sub).csv("Files/tmp/subreddits.csv")
df_key = spark.read.schema(schema_key).csv("Files/tmp/keywords.csv")

sub_rows = df_sub.take(df_sub.count())
key_rows = df_key.take(df_key.count())

comment_list_indirect = []

for index, srow in enumerate(sub_rows):
    subreddit = reddit.subreddit(srow.subreddits)

    for index, krow in enumerate(key_rows):
         
        for submission in reddit.subreddit(srow.subreddits).new(limit=200):
            try:
                submission.comments.replace_more(limit=200)  # get all comments 56
                for comment in submission.comments.list():
                    if comment.created_utc > 1735689600: 
                        if krow.keywords.lower() in comment.body.lower():
                            comment_list_indirect.append((
                                submission.id,
                                comment.id,
                                krow.keywords,
                                comment.body,
                                comment.score,
                                comment.permalink,
                                comment.created_utc,
                            ))
            except Exception as e:
                print(f"    Skipped a submission due to error: {e}")

df_comments_indirect = spark.createDataFrame(comment_list_indirect, schema = schema_comments)
df_comments_indirect.write.format("delta").mode("overwrite")\
                    .option("overwriteSchema", True).saveAsTable(b_comments_indirect)

display(df_comments_indirect) #testing

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

output = {
    "b_submissions": b_submissions,
    "b_comments_indirect": b_comments_indirect,
    "b_comments_direct":  b_comments_direct,
    "s_comments": s_comments,
    "s_submissions": s_submissions,
    "g_comments": g_comments,
    "g_submissions": g_submissions,
    "same_sub": same_sub,
    "same_key": same_key,
    "updates" : updates
}

mssparkutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# spark.sql("DROP TABLE IF EXISTS submissions")
# spark.sql("DROP TABLE IF EXISTS comments_direct")
# spark.sql("DROP TABLE IF EXISTS comments_indirect")
# 
# mssparkutils.fs.rm("Tables/submissions", recurse=True)
# mssparkutils.fs.rm("Tables/comments_direct", recurse=True)
# mssparkutils.fs.rm("Tables/comments_indirect", recurse=True)

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
