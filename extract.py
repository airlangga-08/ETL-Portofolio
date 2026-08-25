import kagglehub
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Extract") \
    .getOrCreate()

def extract(dataset="rifatalam3/coffee-revenue-dataset", csv_filename="Coffe_sales.csv"):
    '''
    Mengunduh dataset dari kagglehub dan membacanya menjadi spark dataframe

    Proses yang dilakukan:
    - Mengunduh dataset dari kagglehub menggunakan identifier dataset
    dengan format ('username/nama-dataset')
    - Menampilkan path folder tempat dataset tersimpan dan daftar
    file di dalamnya sebagai informasi/log
    - Menggabungkan path folder dengan nama file csv yang dituju
    untuk mendapatkan path lengkap file csv
    - Membaca file csv tersebut menjadi Spark dataframe
    dengan baris pertama sebagai header dan tipe data kolom 
    dideteksi otomatis(inferschema)

    '''
    path = kagglehub.dataset_download(dataset)
    print("folder path:", path)
    print("dataset:", os.listdir(path))

    csv_path = os.path.join(path, csv_filename)
    
    df = spark.read.csv(
        csv_path,
        header=True,
        inferSchema=True
    )

    return df
if __name__=="__main__":
    df =extract()
    df.show(5)
    df.printSchema()

    df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/airflow/data/sales_extracted")