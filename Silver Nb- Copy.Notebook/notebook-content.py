# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d3530b5-2362-410f-91cb-5babb8a0fc6d",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "306a4bc8-b6a0-47ec-9db2-ac7425606782",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d3530b5-2362-410f-91cb-5babb8a0fc6d"
# META         },
# META         {
# META           "id": "14de013d-3e56-4849-9b66-7fffc391905c"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

b_submissions = ""
b_comments_direct = ""
b_comments_indirect = ""
s_comments = ""
s_submissions = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, desc, from_unixtime, from_utc_timestamp, concat, lit
from pyspark.sql.types import IntegerType
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# df_comments_direct = spark.read.table("Bronze_LH.comments_direct")
# df_comments_indirect = spark.read.table("Bronze_LH.comments_indirect")
# df_submissions = spark.read.table("Bronze_LH.submissions")

# CELL ********************

df_comments_direct = spark.read.table(b_comments_direct)
df_comments_indirect = spark.read.table(b_comments_indirect)
df_submissions = spark.read.table(b_submissions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_comments_union = df_comments_direct.union(df_comments_indirect)
df_comments_union = df_comments_union.sort(desc(col("time_utc")))
df_comments = df_comments_union.drop_duplicates(["comment_id"])
df_comments = df_comments.where(~col("comment_body").contains("[deleted]"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def url_setup(permalink):
    return concat(lit("https://www.reddit.com"),permalink)


df_comments = df_comments.withColumn("URL",url_setup(col("permalink")))\
                            .drop(df_comments.permalink)

df_submissions = df_submissions.withColumn("URL",url_setup(col("PermaLink")))\
                            .drop(col("PermaLink"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def time_utc(value):
    return from_unixtime(value.cast("bigint"))
    
def time_est(value):
    return from_utc_timestamp(value, "America/New_York")

df_comments = df_comments.withColumn("time_utc",time_utc(col("time_utc")))\
                            .withColumn("time_est",time_est(col("time_utc")))

df_submissions = df_submissions.withColumn("time_utc",time_utc(col("time_utc")))\
                            .withColumn("time_est",time_est(col("time_utc")))

df_submissions.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(s_submissions)
df_comments.write.format("delta").option("overwriteSchema",True)\
                .mode("overwrite").saveAsTable(s_comments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# df_comments = spark.read.table("Silver_LH.comments")
# df_submissions = spark.read.table("Silver_LH.submissions")
# display(df_comments)
# display(df_submissions)

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
