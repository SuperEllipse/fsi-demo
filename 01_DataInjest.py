from pyspark.sql import SparkSession
from src.schema import tbl_schemas
import os
import cml.data_v1 as cmldata

db_name= os.environ["DBNAME"]
tbl_name = []

#db_path =   "s3a://vr-uat1/warehouse/tablespace/external/hive"
#path_prefix = "s3a://vr-uat1/data/bank/"

db_path = os.environ["DBPATH"]
path_prefix = os.environ["PATHPREFIX"]

#spark = (SparkSession
#    .builder
#    .appName("bank-demo")
#    .config("spark.sql.warehouse.dir", db_path)
#    .config("spark.hadoop.fs.s2a.s3guard.ddb.region", "us-east-1")
#    .config("spark.yarn.access.hadoopFileSystems","s3a://vr-uat1/")
#    .master("local[5]") # should be possible to change this to SPARK on Yarn or SPARK on Kubernetes
#    .getOrCreate())



CONNECTION_NAME = os.environ["CONNECTION_NAME"]
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()



#Create a demo DB if not exists
#spark.sql("".join(["DROP DATABASE IF EXISTS ", db_name," CASCADE"]))
sql_string = "".join(["CREATE DATABASE IF NOT EXISTS ", db_name])
spark.sql(sql_string)

def create_managed_table(schema, path, table_name):

    df = (spark.read.format("csv")
        .option("header", "true")
        .option("schema", "schema")
        .option("nullValue", "NA")  
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:SS")
        .option("mode", "failfast")
        .option("path", path)
        .load())
    
    (df.write 
    .mode("overwrite")
#   .option("path", db_path )
    .saveAsTable(db_name + "." + table_name, format="parquet"))
#    spark.sql("REFRESH TABLE " + db_name + "." + table_name).show()
    spark.sql("SELECT * from " + db_name + "." + table_name).show(3)
    return df


for index, dict in tbl_schemas.items(): 
    _ =  create_managed_table(tbl_schemas[index]["schema"], path_prefix+ tbl_schemas[index]["path"],  tbl_schemas[index]["table"])
   # db1.show(10)

spark.sql(f"SHOW TABLES FROM {db_name}").show()

spark.sql(f"SELECT * FROM {db_name}.trans").show()