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
# META     }
# META   }
# META }

# MARKDOWN ********************

# Imports

# CELL ********************

import pyodbc, os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Vault defs

# CELL ********************

df_creds = spark.read.parquet('Files/creds')

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]


vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

password = client.get_secret("sql-server-password").value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# DB defs

# CELL ********************


table_name = ["submissions","comments"]

db = "subcrawler"

jdbc_url = "jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# DB and table check

# CELL ********************


def converts(spark_type):
    spark_type_name = spark_type.simpleString()
    match spark_type_name:
        case "int":
            return "INT"
        case "string":
            return "NVARCHAR(255)"  # Using NVARCHAR as requested
        case "timestamp":
            return "DATETIME"
        case "double":
            return "FLOAT"
        case "boolean":
            return "BIT"
        case "decimal":
            return "DECIMAL(18,2)"
        case _:
            return "NVARCHAR(255)"  # Default for unsupported types


with pyodbc.connect(conn_str_master, autocommit=True) as conn:
    with conn.cursor() as cursor:
        cursor.execute("""
            if not exists(select name from sys.databases where name = 'subcrawler')
            begin
            SELECT 'Database subcrawler doesnt exist.' 
            end
            else
            begin
            SELECT 'Database subcrawler already exists.'
            end
        """)
        result = cursor.fetchone()
        print(result[0])

for table in table_name:

    df = spark.read.table(f'Gold_LH.{table}')
    print(f"table is [{table}]")

    if table == "submissions":
        df = df.withColumnRenamed("key","keyword")

    cols = [f"{field.name} {converts(field.dataType)}" for field in df.schema.fields]

    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                IF NOT EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                BEGIN
                    SELECT ? + ' does not exist.';
                    EXEC('CREATE TABLE [' + ? + '] (' + ? + ');');
                    SELECT ? + ' created.';
                END
                ELSE
                BEGIN
                    SELECT ? + ' already exists' 
                END
            """, (table, f'[{table}]', table, ','.join(cols), f'[{table}]', f'[{table}]'))

            while True:
                result = cursor.fetchone()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Write

# CELL ********************


for table in table_name:

    df = spark.read.table(f'Gold_LH.{table}')
    print(table,' : ',df.count(),'rows')
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote data to RDS table [{table}].")
    except Exception as e:
        print(f"Failed to write to RDS: {e}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Testing

# CELL ********************

#testing...

'''
for table in table_name:
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            print(table)
            cursor.execute("""
                IF EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                BEGIN
                    EXEC ('SELECT COUNT (*) FROM [' + ? + ']')
                END
                ELSE
                BEGIN
                    SELECT ? + ' doesn''t exist';
                END
            """, (table, table, table))
            
            result = cursor.fetchall()

            while True:
                if result:
                    print(result[0])
                if not cursor.nextset():
                        break

'''

'''

for table in table_name:
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                    IF EXISTS (SELECT name FROM sys.tables where name = ?)
                    BEGIN
                    EXEC('DROP TABLE' + ' ['+ ? +']');
                    SELECT ? + ' dropped';
                    END
                    ELSE
                    BEGIN
                    SELECT ? + ' doesnt exist';
                    END
            """,(table, table, f'[{table}]', f'[{table}]'))

            result = cursor.fetchall()

            while True:
                if result:
                    print(result[0])
                if not cursor.nextset():
                        break
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
