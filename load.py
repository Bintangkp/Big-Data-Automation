from pymongo import MongoClient
import pyspark
from pyspark.sql import SparkSession

def load_data(input_path: str, mongo_uri: str, database_name: str, collection_name: str):
    '''
    Function untuk ngeload data ke mongodb
    Contoh pemanggilan fungsi:
    load_data(input_path, 
              mongo_uri,
              database_name, 
              collection_name)
    '''
    spark = SparkSession.builder.appName("Load_Data").getOrCreate()
    
    # Baca data yang telah ditransformasi
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.toPandas()

    # Koneksi ke MongoDB
    client = MongoClient(mongo_uri)
    db_client = client[database_name]
    collection = db_client[collection_name]

    # Konversi DataFrame ke list of dictionary dan simpan ke MongoDB
    document_list = df.to_dict(orient='records')
    collection.insert_many(document_list)

load_data("/opt/airflow/dags/data_cleaned", 
        "mongodb+srv://mongodb:mongodb@bintang-playground.fz8nm.mongodb.net/",
        "MongoDB_Milestone3", 
        "hasil_transform")
