from pyspark.sql import SparkSession
import pandas as pd
import os

def spark_session():
   spark = SparkSession.builder \
      .appName("Read csv file ") \
      .getOrCreate()
   return spark

def delete_files(write_directory):
   for filename in os.listdir(write_directory):
      file_path = os.path.join(write_directory, filename)
      if os.path.isfile(file_path):
         os.remove(file_path)
         print(f"Deleted the existing file: {file_path}")
      else:
         print("Files not exist")

def create_directory(write_directory):
   os.makedirs(write_directory, exist_ok=True)

def read_and_write_files(read_directory,write_directory,file_name):
   files = os.listdir(read_directory)

# Display files
   for file in files:
       pandas_df = pd.read_excel(f'{read_directory}/{file}', engine='openpyxl')
       csv_file_path = f"{write_directory}/{file_name}"
       file_exists = os.path.isfile(csv_file_path)
       pandas_df.to_csv(csv_file_path,sep=",",header=not file_exists,index=False,mode="a")

def read_csv_spark(spark,write_directory,file_name):
   csv_file_path = f"{write_directory}/{file_name}"
   return spark.read.options(header="true",inferSchema='True',delimiter=',').csv(csv_file_path)

if __name__ == "__main__":
   read_directory=input("Enter directory from where you want to read with forward / : ")
   write_directory=input("Enter directory to where you want to write files / : ")
   file_name=input("Enter csv file name you want to write with .csv extension : ")

   create_directory(write_directory)
   delete_files(write_directory)

   read_and_write_files(read_directory,write_directory,file_name)

   spark=spark_session()

   POT_df = read_csv_spark(spark,write_directory,file_name)


