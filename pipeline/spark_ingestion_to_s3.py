import zipfile

import os
import glob
from spark import spark

def extract_data():

    # Extract brazilian-ecommerce.zip file and store in folder data
    with zipfile.ZipFile('/home/nhut/etl-airflow/pipeline/data/brazilian-ecommerce.zip', 'r') as zip_ref:
        zip_ref.extractall('/home/nhut/etl-airflow/pipeline/data/csv/')



    # Get all csv files in folder data
    path = '/home/nhut/etl-airflow/pipeline/data/csv'
    csv_files = glob.glob(path + "/*.csv")



    # read CSVs into PySpark DataFrame
    for csv_file in csv_files:
        df = spark.read.csv(csv_file, header= True)
        # write df to parquet file and store in aws s3
        head, tail = os.path.split(csv_file)
        name_file = tail.split('.')[0]
        df.write.mode("overwrite").parquet("s3a://olistsalesbucket/" + name_file + ".parquet") 

    spark.stop()
if __name__ =="__main__":
    extract_data()