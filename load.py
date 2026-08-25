from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("Data Load") \
    .getOrCreate()

# Menghubungkan ke Mongodb Atlas
def load_data(df):
    '''
    Membuat load data hasil transformasi ke dalam database MongoDB

    Proses yang dilakukan:
    - Membuka koneksi ke MongoDB Atlas menggunakan MongoClient
    - Memilih database 'coffee_sales_db' dan collection 'coffee_sales'
    sebagai tujuan penyimpanan data
    - Merubah kolom 'date' menjadi tipe string dengan format
    yyyy-MM-dd, karena tipe timestamp dari spark tidak bisa
    langsung disimpan ke MongoDB
    - Mengubah Spark Dataframe menajadi list of dictionary agar
    bisa diproses oleh Pymongo (setiap baris menjadi satu dokumen)
    - Menyimpan seluruh data ke collection MongoDB menggunakan 
    insert_many(), hanya jika data tidak kosong
    - Menutup koneksi ke MongoDB setelah prose selesai

    '''
    client = MongoClient(
        "mongodb+srv://mongodb:mongodb@coda20.iihbtmq.mongodb.net/?appName=coda20"
    )

    # Memilih database dan collection
    db = client["coffee_sales_db"]
    collection = db["coffee_sales"]

    # Mengubah tipe data menjadi string agar bisa disimpan di mongodb
    df = df.withColumn(
        "date",
        F.date_format(F.col("date"), "yyyy-MM-dd")
    )
    # Mengubah Spark Dataframe menjadi list of dictionary
    data = [row.asDict() for row in df.collect()]

    # Menyimpan data ke MongoDB
    if data:
        collection.insert_many(data)

    client.close()

if __name__=="__main__":
    # Membaca hasil transform menggunakan Pyspark
    df = spark.read.option("header",True).option("inferSchema",True).csv("/opt/airflow/data/sales_transformed")
    

    df.show(5)
    df.printSchema()

    #Load ke MongoDB
    load_data(df)