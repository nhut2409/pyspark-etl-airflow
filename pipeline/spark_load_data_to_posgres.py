from spark import spark



def load_data():
    bucket_url = "s3a://olistsalesbucket/"
    missed_deadline_parfile= "late_carrier_deliveries.parquet"
    df_missed_deadline_job = spark.read.parquet(bucket_url + missed_deadline_parfile)
    print(df_missed_deadline_job)
    df_missed_deadline_job.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/olist_warehouse_db") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "olist_missed_deadline_tb") \
    .option("user", "airflow_psql").option("password", "airflow").mode("overwrite").save()  
    
    
if __name__ =="__main__":    
    load_data()
    
    