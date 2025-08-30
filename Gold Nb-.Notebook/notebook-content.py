# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "426f53f6-c160-4a27-8dab-443ac08514a6",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "7e1d42ff-c288-4d0c-b15d-bf94da9da4b8",
# META       "known_lakehouses": [
# META         {
# META           "id": "dc9d3bc7-2078-4e0b-9d63-97f8b02c795b"
# META         },
# META         {
# META           "id": "4e947ca6-aebd-445c-b8b5-949389450fd0"
# META         },
# META         {
# META           "id": "426f53f6-c160-4a27-8dab-443ac08514a6"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "a34f4b34-31fb-92d1-4c68-3ab0389d79ba",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

s_comments = ""
s_submissions = ""
g_comments = "" 
g_submissions = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from textblob import TextBlob
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# df_comments = spark.read.table("Silver_LH.comments")
# df_submissions = spark.read.table("Silver_LH.submissions")

# MARKDOWN ********************

# pip install textblob

# CELL ********************

df_comments = spark.read.table(s_comments)
df_submissions = spark.read.table(s_submissions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sentiment_udf = udf(lambda text: TextBlob(text).sentiment.polarity, FloatType())

df_comments = df_comments.withColumn("comment_sentiment", sentiment_udf(col("comment_body")))
df_submissions = df_submissions.withColumn("title_sentiment", sentiment_udf(col("post_title")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_comments.write.format("delta").mode("overwrite").saveAsTable(g_comments)
df_submissions.write.format("delta").mode("overwrite").saveAsTable(g_submissions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
