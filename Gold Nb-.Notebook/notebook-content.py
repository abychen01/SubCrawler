# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e211a6b-12cf-4545-82bb-64c49e7d785e",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "2a8af919-0041-46ee-b6c9-e0fcee3bb1c7",
# META       "known_lakehouses": [
# META         {
# META           "id": "3561e974-6566-4287-a7a3-63458f55b871"
# META         },
# META         {
# META           "id": "9e211a6b-12cf-4545-82bb-64c49e7d785e"
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
