from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-19.jdk/Contents/Home"

conf = SparkConf().setAppName("example").setMaster("local").set("spark.driver.extraClassPath", "./jars/*")

sc = SparkContext.getOrCreate(conf)
spark = SparkSession(sc)

df = spark.read.options(delimiter=",", header=True).csv("./data/AdvWorksData.csv")

df.createOrReplaceTempView("sales")
output = spark.sql("SELECT * from sales where productsubcategory='Caps'")

tbl = "public.sales_table"
database = "testdb"
password = "root"
user = "postgres"

df.write.mode("overwrite").format("jdbc") \
    .option("url", f"jdbc:postgresql://localhost:5432/{database}") \
    .option("dbtable", tbl).option("user", user).option("password", password) \
    .option("driver", "org.postgresql.Driver").save()
