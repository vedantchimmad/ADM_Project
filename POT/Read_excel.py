from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, month, year,to_date,concat,datediff,sum,avg
import pandas as pd
import os
import logging

def get_logger():
    """
    Generic logger function
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(module)s[%(filename)s:%(lineno)d] %(message)s',
        datefmt='%y/%d/%m %H:%M:%S',
        level=logging.INFO)
    return logging

def spark_session(logging):
    """
    Generic spark session
    """
    spark = SparkSession.builder \
      .appName("Read csv file ") \
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
      .getOrCreate()
    logging.info("SparkSession is created")
    return spark

def delete_files(write_directory,logging):
   for filename in os.listdir(write_directory):
      file_path = os.path.join(write_directory, filename)
      if os.path.isfile(file_path):
         os.remove(file_path)
         logging.info(f"Deleted the existing file: {file_path}")
      else:
         logging.info("Files not exist")

def create_directory(write_directory,logging):
   os.makedirs(write_directory, exist_ok=True)
   logging.info("Required directory to store single csv file is created")

def read_and_write_files(read_directory,write_directory,file_name,logging):
   files = os.listdir(read_directory)

# Display files
   for file in files:
       pandas_df = pd.read_excel(f'{read_directory}/{file}', engine='openpyxl')
       csv_file_path = f"{write_directory}/{file_name}"
       file_exists = os.path.isfile(csv_file_path)
       pandas_df.to_csv(csv_file_path,sep=",",header=not file_exists,index=False,mode="a")

   logging.info("single csv file is created successfully")
def read_csv_spark(spark,write_directory,file_name,logging):
   csv_file_path = f"{write_directory}/{file_name}"
   df=spark.read.options(header="true",inferSchema='True',delimiter=',').csv(csv_file_path)
   logging.info("Reading single csv file")
   return df

def POT_transformation(df,logging):
    transactionId_count = df.groupBy("TransactionID").count()
    df=df.withColumn("Tower",lit("S2P")) \
      .withColumn("KPI Name",lit("POT%")) \
      .withColumn("Period (M/Y)",concat(month(to_date(col("InvoiceDate"), "MM/dd/yyyy")),lit("/"),year(to_date(col("InvoiceDate"), "MM/dd/yyyy")))) \
      .withColumn("Segment",col("SEGMENT")) \
      .withColumn("Country",lit(None)) \
      .withColumn("Entity",col("DocumentCompany")) \
      .withColumn("Vendor Name",col("Supplier Name")) \
      .withColumn("Direct/Indirect Expense",lit(None)) \
      .withColumn("Supplier Type",lit(None)) \
      .withColumn("Payment Term Code",lit(None)) \
      .withColumn("Payment Term Description",lit(None)) \
      .withColumn("Mode of payment",lit(None)) \
      .withColumn("PO/Non-PO",lit(None)) \
      .withColumn("Critical vendor (Y/N)",lit(None)) \
      .withColumn("Commodity/Non-commodity",col("commodity?")) \
      .withColumn("POT Status",when(datediff(to_date(col("PaidDate"),"MM/dd/yyyy"), to_date(col("NetDueDate"),"MM/dd/yyyy")) > 3, "Late").when(datediff(to_date(col("PaidDate"),"MM/dd/yyyy"), to_date(col("NetDueDate"),"MM/dd/yyyy")) < -3, "Early").otherwise("POT")) \
      .withColumn("Avg Paid Days",datediff(to_date(col("PaidDate"), "MM/dd/yyyy"),to_date(col("NetDueDate"), "MM/dd/yyyy"))) \
      .withColumn("Target Met/Not Met",col("POT Status")) \
      .join(transactionId_count, on="TransactionID", how="left").withColumnRenamed("count", "Den-Total Inv Count") \
      .withColumn("Total Amount (USD)",lit(df.agg(sum("GrossAmountUsd")).collect()[0][0])) \

    POT_df=df.withColumn("Num-Inv Count-KPI Met",lit(df.filter(col("POT Status") == "POT").count())) \
      .withColumn("Total Amount (USD)-KPI Met",concat(col("Total Amount (USD)"),lit("-"),col("POT Status"))) \
      .withColumn("Target",lit(None)) \
      .withColumn("Avg aging days",lit(df.select(avg("Avg Paid Days")).collect()[0][0]))
    logging.info(f"POT DF {POT_df.limit(1).show()}")
    return POT_df

def select_required_columns(POT_df,logging):
    final_df=POT_df.select("Tower","KPI Name","Period (M/Y)","Region","Segment","Country","System","Entity","Vendor Name","Vendor No","Direct/Indirect Expense","Supplier Type","Payment Term Code","Payment Term Description","Mode of payment","Transaction Type","PO/Non-PO","Critical vendor (Y/N)","Commodity/Non-commodity","Target Met/Not Met","Den-Total Inv Count","Total Amount (USD)","Num-Inv Count-KPI Met","Total Amount (USD)-KPI Met","Target","Avg aging days")
    logging.info(f"Final df {final_df.limit(1).show()}")
    return final_df
def create_view(final_df,view_name,logging):
    final_df.createOrReplaceGlobalTempView(view_name)
    logging.info(f"Global view is created successfully {view_name}")
if __name__ == "__main__":
   read_directory=input("Enter directory from where you want to read with forward / : ")
   write_directory=input("Enter directory to where you want to write files / : ")
   file_name=input("Enter csv file name you want to write with .csv extension : ")
   view_name=input("Enter view name you want to create : ")

   logging = get_logger()

   logging.info("Creating directory ")
   create_directory(write_directory,logging)

   logging.info("Deleting the existing files from write directory only ")
   delete_files(write_directory,logging)

   read_and_write_files(read_directory,write_directory,file_name,logging)

   spark=spark_session(logging)

   POT_df = read_csv_spark(spark,write_directory,file_name,logging)

   POT_df = POT_transformation(POT_df,logging)

   final_df = select_required_columns(POT_df,logging)

   logging.info("Creating the view")
   create_view(final_df,view_name,logging)

   df = spark.sql(f"SELECT * FROM global_temp.{view_name}")
   df.show()

