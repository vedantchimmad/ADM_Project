
def read_file(input_files_folder):
    dataframes_list=[]
    for file_name in dbutils.fs.ls(input_files_folder):
        filepath=file_name.path
        df=spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(filepath)
        dataframes_list.append(df)
    return dataframes_list
def create_file(input_files_folder,output_file_folder):
    list_dateaframe = read_file(input_files_folder)
    final_df=list_dateaframe[0]
    for df in list_dateaframe[1:]:
        final_df=final_df.unionByName(df,allowMissingColumns=True)

    final_df.write.format("csv").mode('overwrite').save(output_file_folder)

if __name__ == "__main__":
    input_files_folder=input("Enter excel files folder : ")
    output_file_folder=input("Enter table name along with catalog schema table")

    create_file(input_files_folder,output_file_folder)