from pyspark.sql import DataFrame
from functools import reduce
def read_excel(files_folder):
    dataframes_list=[]
    for file_name in dbutils.fs.ls(files_folder):
        filepath=file_name.path
        df=spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(filepath)
        dataframes_list.append(df)

    return dataframes_list
def combined_excel(dataframes_list):
    return reduce(DataFrame.union,dataframes_list)
def create_table(files_folder,output_table):
    dataframe_list= read_excel(files_folder)
    combined_df=combined_excel(dataframe_list)
    combined_df.write.format("delta").mode("overwrite").saveAsTable(output_table)

if __name__ == "__main__":
    files_folder=input("Enter excel files folder : ")
    catalog_schema_table=input("Enter table name along with catalog schema table")
    create_table(files_folder,catalog_schema_table)