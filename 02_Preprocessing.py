
from pyspark.sql import SparkSession
import os
import cml.data_v1 as cmldata
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, BooleanType, DateType, IntegerType
import numpy as np


db_name= os.environ["DBNAME"]
tbl_name = []

#db_path =   "s3a://vr-uat1/warehouse/tablespace/external/hive"
#path_prefix = "s3a://vr-uat1/data/bank/"

db_path = os.environ["DBPATH"]
path_prefix = os.environ["PATHPREFIX"]
tbl_name = []


CONNECTION_NAME = os.environ["CONNECTION_NAME"]
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# Write simple SQL query to check that 

# Checking that there are no orphaned account IDs (without Owners)
sql_string = f"""
    SELECT A.*, B.account_id  FROM {db_name}.account A
        LEFT OUTER JOIN {db_name}.disposition B ON A.account_id = B.account_id WHERE B.account_id IS NULL """

spark.sql(sql_string).show()

## You can remove IS NULL above to see the difference

# check that Every record in Order and Loan table also exists as a transaction
spark.sql(f"""
    
           SELECT loan_ds.loan_id, loan_ds.account_id, loan_ds.date, loan_ds.amount, loan_ds.duration, loan_ds.payments, loan_ds.status, trans_ds.account_id,ROUND(trans_ds.total,0 ) as total FROM {db_name}.loan AS loan_ds
            LEFT OUTER JOIN 
          ( SELECT  DISTINCT account_id, sum(amount) as total  FROM  {db_name}.trans WHERE K_symbol = 'UVER'  GROUP BY account_Id , balance) AS  trans_ds
         ON loan_ds.account_id = trans_ds.account_id
         WHERE trans_ds.account_id IS NULL     
          """).show()

# Uncomment the line WHERE trans_ds.account_id ISS 


# Do some basic null checks on key tables
df_trans_acct = spark.sql(
    f""" 
        SELECT "trans" as Tablename , "account_Id", count(account_id) AS Total FROM {db_name}.trans WHERE account_id IS NULL
    """)
df_trans_date = spark.sql(
    f""" 
        SELECT "trans" as Tablename , "date", count(date) AS Total FROM {db_name}.trans WHERE date IS NULL
    """)

df_trans_acct.union(df_trans_date).show()


#Preprocessing Step 1: Birthnumber is an encoded variable
#Birth number is an encoded variable containing Birthday and Sex: 
# Value : YYMMDD ( for Men)
# Value YYMM+50DD ( for Women)

# But the real data shows that men are sometimes encoded with +50 and also Women are sometimes not. eg. Client id 18, Client ID 7
#+---------+------+------------+-----------+
#|client_id|gender|birth_number|district_id|
#+---------+------+------------+-----------+
#         7|Female|      290125|         15|
#        18|  Male|      315405|         76|


client_df = spark.sql(f"SELECT * from {db_name}.client")

# So we apply some generic transform to change this encoded variable
client_df = client_df.withColumn("birth_numberU", 
#          when((col("gender") == "Female") & (col("birth_number").substr(3,2) > 50), col("birth_number") - 5000)
          when((col("birth_number").substr(3,2) > 50), col("birth_number") - 5000)
         .otherwise( col("birth_number")).cast(IntegerType()))

client_df = client_df.withColumn("birth_date", to_date(concat(lit("19"), col("birth_numberU").cast(StringType())), "yyyyMMdd"))
client_df = client_df.drop(col("birth_numberU"))
client_df.show()


# Check if we have  some issues with data
null_df = client_df.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull(), c \
                           )).alias(c)
                    for c in client_df.columns])

null_df.show()


# let us now merge the client, disp and loans data
disp_df = spark.sql(f"SELECT * from {db_name}.disposition")
loan_df = spark.sql(f"SELECT * FROM {db_name}.loan")

disp_df.show(10)
loan_df.show(10)

loan_full_df = (client_df.join(disp_df, client_df.client_id == disp_df.client_id , "inner")
                         .join(loan_df, disp_df.disp_id == loan_df.loan_id, "inner")).drop(disp_df.client_id).drop(disp_df.account_id)
loan_full_df.show()

# Transformation : We now need to transform the Type type, Operation, K_symbol fields to readable formats

# REFACTOR : Subsequent refactor to a package function

@udf(returnType=StringType())    
def convert_trans_type_to_eng(x):
    if x == 'PRIJEM':
        return 'Credit'
    elif x == 'VYDAJ':
        return 'Withdrawal'
    else:
        return np.NaN
    
@udf(returnType=StringType())    
def convert_trans_op_to_eng(x):
    if x == 'VYBER KARTOU':
        return 'Credit card withdrawal'
    elif x == 'VKLAD':
        return 'Credit in cash'
    elif x == 'PREVOD Z UCTU':
        return 'Collection from another bank'
    elif x == 'VYBER':
        return 'Withdrawal in Cash'
    elif x == 'PREVOD NA UCET':
        return 'Remittance to another bank'    
    else:
        return np.NaN
@udf(returnType=StringType())        
def convert_trans_k_symbol_to_eng(x):
    if x == 'POJISTNE':
        return 'Insurance payment'
    elif x == 'SLUZBY':
        return 'Payment for statement'
    elif x == 'UROK':
        return 'Interest credited'
    elif x == 'SANKC. UROK':
        return 'Sanction interest if negative balance'
    elif x == 'SIPO':
        return 'Household'
    elif x == 'DUCHOD':
        return 'Old-age pension'  
    elif x == 'UVER':
        return 'Loan payment'      
    else:
        return np.NaN
    
trans_df = spark.sql(f"SELECT * FROM {db_name}.trans WHERE operation IS NOT NULL")

# the transaction DF now has all these encoded values in English for dashboarding
trans_df = trans_df.withColumn("trans_date",  to_date(concat(lit("19"), col("date").cast(StringType())), "yyyyMMdd")) \
        .select("trans_id", "account_id", "date", "type", "operation", "amount", "balance", "k_symbol", "bank", "account", "trans_date", \
                        convert_trans_type_to_eng(col("type")).alias("type_eng" ), \
                        convert_trans_op_to_eng(col("operation")).alias("operation_eng"), \
                        convert_trans_k_symbol_to_eng(col("k_symbol")).alias("k_symbol_eng"))
trans_df.show()



#Need to decode Order table as well 
order_df = spark.sql(f"Select * from {db_name}.orders")
@udf(returnType=StringType())
def convert_order_k_symbol_to_eng(x):
    if x == 'POJISTNE':
        return 'Insurance payment'
    elif x == 'SIPO':
        return 'Household'
    elif x == 'LEASING':
        return 'Leasing'
    elif x == 'UVER':
        return 'Loan payment'
    else:
        return np.NaN
    
order_df = order_df.withColumn("k_symbol_eng",  convert_order_k_symbol_to_eng(col("k_symbol")))

#finally We need to write all this data as a cleaned dataset
(client_df.write 
          .mode("overwrite")
#          .option("path", db_path )
          .saveAsTable(db_name + "." + "client_cleaned", format="parquet"))
#spark.sql("REFRESH TABLE " + db_name + "." + "client_cleaned")
spark.sql("SELECT * from " + db_name + "." + "client_cleaned").show(3)

(trans_df.write 
           .mode("overwrite")
#           .option("path", db_path )
           .saveAsTable(db_name + "." + "trans_cleaned", format="parquet"))
# spark.sql("REFRESH TABLE " + db_name + "." + "trans_cleaned")
spark.sql("SELECT * from " + db_name + "." + "trans_cleaned").show(3)

(loan_full_df.write 
           .mode("overwrite")
#   .option("path", db_path )
           .saveAsTable(db_name + "." + "loan_full_cleaned", format="parquet"))
# #    spark.sql("REFRESH TABLE " + db_name + "." + table_name).show()
spark.sql("SELECT * from " + db_name + "." + "loan_full_cleaned").show(3)

(order_df.write 
           .mode("overwrite")
#   .option("path", db_path )
           .saveAsTable(db_name + "." + "order_cleaned", format="parquet"))
# #    spark.sql("REFRESH TABLE " + db_name + "." + table_name).show()
spark.sql("SELECT * from " + db_name + "." + "order_cleaned").show(3)