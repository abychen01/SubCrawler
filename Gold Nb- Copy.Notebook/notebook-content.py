# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fcacb96a-4fd9-413a-af07-7e7b84dfee79",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "306a4bc8-b6a0-47ec-9db2-ac7425606782",
# META       "known_lakehouses": [
# META         {
# META           "id": "fcacb96a-4fd9-413a-af07-7e7b84dfee79"
# META         },
# META         {
# META           "id": "0d3530b5-2362-410f-91cb-5babb8a0fc6d"
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
