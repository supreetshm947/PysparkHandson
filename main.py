from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os

os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-18.0.1.1"

conf = SparkConf().setAppName("example").setMaster("local").set("spark.driver.extraClassPath", "./jars/*")\
    .set("spark.executor.extraClassPath", "./jars/*")

sc = SparkContext.getOrCreate(conf)
spark = SparkSession(sc)

database = "AdventureWorks2019"
table = "Person"
# password = os.environ["PGPASS"]
# user = os.environ["PGUID"]
schema = "Person"

jdbc_url = f"jdbc:sqlserver://localhost:1433;database={database};encrypt=true;trustServerCertificate=true;integratedSecurity=true"

df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable",f"{schema}.{table}") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load()
    #.option("user",user).option("password", password)\


print(df.show())
