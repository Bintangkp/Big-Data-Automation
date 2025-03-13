import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp
from pyspark.sql.types import BooleanType

def transform_data(input_path: str, output_path="/opt/airflow/dags/data_cleaned"):
    '''
    Function untuk mengubah data sesuai transformasi yg dibutuhkan
    Contoh pemanggilan fungsi:
    transform_data('data_raw')
    '''
    spark = SparkSession.builder.appName("transform_data").getOrCreate()

    # Baca data yang diekstrak
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Transformasi data
    #Isi null value pada item dengan Unknown 
    df = df.fillna({"Item": "Unknown"})

    #Isi null value dengan false dan pastikan menjadi boolean 
    df = df.withColumn("Discount Applied", 
                       when(col("Discount Applied").isNull(), False)
                       .otherwise(col("Discount Applied").cast(BooleanType())))

    #Isi sisa null value dengan 0
    df = df.fillna(0)
    
    #Ganti tipe data ke timestamp
    df = df.withColumn("Transaction Date", to_timestamp(df["Transaction Date"], "yyyy-MM-dd"))

    # Rename semua kolom: lowercase dan menggunakan snakecase
    df = df.toDF(*[col.lower().replace(" ", "_") for col in df.columns])

    # Simpan data yang telah ditransformasi
    df.repartition(1).write.csv(output_path, header=True, mode="append")

transform_data('/opt/airflow/dags/data_raw')