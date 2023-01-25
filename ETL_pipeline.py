from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import os
import config


def run_etl():
    conf = SparkConf().setAppName("example").setMaster("local").set("spark.driver.extraClassPath", "./jars/*")

    sc = SparkContext.getOrCreate(conf)
    spark = SparkSession(sc)

    server = "localhost"

    src_url = f"jdbc:sqlserver://{server}:1433;database={config.src_database};encrypt=true;trustServerCertificate=true" \
              f";integratedSecurity=true "
    target_url = f"jdbc:postgresql://{server}:{config.postgres_port}/{config.target_db}?user={config.postgres_usr}&password={config.postgres_pass}"

    tables = ['Production.Product', 'Production.ProductSubcategory', 'Production.ProductCategory',
              'Sales.SalesTerritory']
    data = extract_table(spark, config.src_driver, config.src_database, src_url)
    load_data(spark, data, target_url, config.target_driver)


def extract_table(session: SparkSession, src_driver, db_name, src_url):
    data = {}
    try:
        query_get_all_tables = f"SELECT t.name AS TableName, s.name AS SchemaName " \
                               f"FROM {db_name}.sys.tables t JOIN " \
                               f"{db_name}.sys.schemas s ON t.schema_id = s.schema_id"
        dfs = session.read.format("jdbc").options(driver=src_driver, url=src_url, query=query_get_all_tables).load()
        for row in dfs.collect():
            table_name = row["TableName"]
            schema_name = row["SchemaName"]
            df = session.read.format("jdbc").options(driver=src_driver, url=src_url, dbtable=f"{schema_name}.{table_name}").load()
            data[f"{schema_name}.{table_name}"] = df
    except Exception as e:
        print(f"Error in Extraction {e}")
    return data


def load_data(session: SparkSession, data, target_url, target_driver):
    for key in data.keys():
        try:
            df = data.get(key)
            key = key.split(".")
            schema = key[0]
            table = key[1]
            df.write.mode("overwrite")

            spark = SparkSession.builder \
                .master("local") \
                .appName("Creating a schema") \
                .config("spark.driver.url", target_url) \
                .config("spark.driver.oracle", config.target_driver) \
                .getOrCreate()

            # creating schema first
            schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"
            print(f"Creating Schema {schema}")
            spark.sql(schema_query)
            print(f"importing {df.count()} rows.... for {schema}.{table}")
            df.write.mode("overwrite").format("jdbc").options(url=target_url, driver=target_driver, dbtable=table).save()
            print(f"Data imported Successfully for table {key}")
        except Exception as e:
            print(f"Error in Loading {e}")


if __name__ == "__main__":
    os.environ["JAVA_HOME"] = config.java_path
    run_etl()
