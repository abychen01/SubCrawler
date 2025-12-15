# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14de013d-3e56-4849-9b66-7fffc391905c",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "306a4bc8-b6a0-47ec-9db2-ac7425606782",
# META       "known_lakehouses": [
# META         {
# META           "id": "fcacb96a-4fd9-413a-af07-7e7b84dfee79"
# META         },
# META         {
# META           "id": "14de013d-3e56-4849-9b66-7fffc391905c"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "e4d37e35-f01f-b1cf-4a95-e6be75052b1d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

b_submissions = ""
b_comments_direct = ""
same_sub = ""
same_key = ""
updates = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from mailjet_rest import Client
from delta.tables import DeltaTable
from pyspark.sql.functions import col,desc
import pprint, os

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

post_count = 0
comment_count = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

mailjet_api_key = client.get_secret("mailjet-api-key").value
mailjet_api_secret = client.get_secret("mailjet-api-secret").value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_submissions = DeltaTable.forName(spark, "submissions")
df_comments = DeltaTable.forName(spark, "comments_indirect")

latest_sub_ver = df_submissions.history(1).select(col("version")).collect()[0][0]
latest_comments_ver = df_comments.history(1).select(col("version")).collect()[0][0]

df_sub_curr_ver = spark.read.table("submissions")
df_comments_curr_ver = spark.read.table("comments_indirect")

print(latest_sub_ver)
print(latest_comments_ver)

if same_sub == True and same_key == True:

    print("same_sub and same_key are TRUE") #testing

    try:

        if updates:

            print("Updates are TRUE")#testing

            df_sub_prev_ver = spark.read.option("versionAsOf",latest_sub_ver - 1).table("submissions")
            post_count = df_sub_curr_ver.count() - df_sub_prev_ver.count() \
                        if df_sub_curr_ver.count() > df_sub_prev_ver.count() else 0

        df_comments_prev_ver = spark.read.option("versionAsOf",latest_comments_ver - 1)\
                                    .table("comments_indirect")
        comment_count = df_comments_curr_ver.count() - df_comments_prev_ver.count() \
                            if df_comments_curr_ver.count() > df_comments_prev_ver.count() else 0

        if latest_sub_ver > 0:

            print(f"more than one version and latest is {latest_sub_ver}")
            print(f"Number of new posts {post_count}")
            print(f"Number of new comments {comment_count}")


        else:
            print("updates are FALSE")#testing
            
            print()

    except Exception as e:

        print(f"only one version and it is {latest_sub_ver}")
        print(e)

else:
    
    post_count = df_sub_curr_ver.count()
    comment_count = df_comments_curr_ver.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_creds = spark.read.parquet('Files/creds')
display(df_creds)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#tesing...
'''

df_sub_prev_ver.show()
display(spark.read.table("submissions"))
df_comments_prev_ver.show()
display(spark.read.table("comments_indirect"))
display(df_comments.history())
display(df_submissions.history())
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_email = spark.read.format("csv").load("Files/tmp/email.csv")
df_email = df_email.collect()[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

post_list = df_sub_curr_ver.select(col("post_title"))\
                            .sort(desc(col("time_utc"))).take(post_count)

comments_list = df_comments_curr_ver.select(col("comment_body"))\
                            .sort(desc(col("time_utc"))).take(comment_count)


post_list_html = "<br>".join(row.post_title for row in post_list)

comments_list_html = "<br>".join(row.comment_body for row in comments_list)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



mailjet = Client(auth=(mailjet_api_key, mailjet_api_secret), version='v3.1')
data = {
  'Messages': [
				{
						"From": {
								"Email": "lonewolfdiesthepackremains@gmail.com",
								"Name": "Me"
						},
						"To": [
								{
										"Email": df_email._c0,
										"Name": "You"
								}
						],
						"Subject": "New update",
						"TextPart": "New update",
						
				}
		]
}

if same_sub == True and same_key == True and ((post_count + comment_count) > 0):

	print ("same_sub == True and same_key == True and ((post_count + comment_count) > 0)")#testing

	data['Messages'][0]['HTMLPart'] = f"<h3>Hello {df_email._c0},</h3><br/><br> \
										New updates are available <br><br>\
										New posts with keyword: {post_count} <br><br>\
										New comments with keyword: {comment_count} <br><br>\
										New posts:  <br> {post_list_html} <br><br>\
										New comments: <br> {comments_list_html} <br><br>\
										Follow the link to view the latest  \
										<a href=\"https://www.google.com/\">Power BI report</a>"
	result = mailjet.send.create(data=data)
	print(result.json())

else:

	print("same_sub == False and same_key == False and ((post_count + comment_count) <= 0)")
	if latest_sub_ver == 0:
		
		print("latest_sub_ver == 0")#testing
		
		data['Messages'][0]['HTMLPart'] = f"<h3>Hello {df_email._c0},</h3><br/> <br>\
										First update <br> <br>\
										New posts:  <br> {post_list_html} <br> <br>\
										New comments: <br> {comments_list_html} <br> <br>\
										Follow the link to view the latest  \
										<a href=\"https://www.google.com/\">Power BI report</a>"
		
		result = mailjet.send.create(data=data)
		print(result.json())



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
