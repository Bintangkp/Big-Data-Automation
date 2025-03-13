import pyspark
from pyspark.sql import SparkSession

def extract_data(input_path: str, output_path="/opt/airflow/dags/data_raw"):
    '''
    Function untuk mengekstrak dataset menjadi data raw yg akan di transform
    Contoh pemanggilan fungsi:
    extract_data("P2M3_Bintang-Ksatrio_data_raw")
    '''
    spark = SparkSession.builder.appName("extract_data").getOrCreate()
    
    # Baca data dari file CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Simpan data ke CSV
    df.repartition(1).write.csv(output_path, header=True, mode="append")

extract_data('/opt/airflow/dags/Dataset.csv')