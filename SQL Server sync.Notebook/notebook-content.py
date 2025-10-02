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
# META           "id": "0bcf40e1-5936-4fdc-af5b-c02a4546065b"
# META         },
# META         {
# META           "id": "9e211a6b-12cf-4545-82bb-64c49e7d785e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet("Files/creds")
password = df2.collect()[0]["password"]

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
# META   "language_group": "synapse_pyspark"
# META }

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
            SELECT 'Database subcrawler doesn't exist.' 
            end
            else
            begin
            SELECT 'Database subcrawler already exists.'
            end
        """)
        result = cursor.fetchone()
        print(result[0])

for table in table_name:

    df = spark.read.table(table)
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

# CELL ********************


for table in table_name:

    df = spark.read.table(table)
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
