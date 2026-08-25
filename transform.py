from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Transform") \
    .getOrCreate()

df = spark.read.csv(
    "/opt/airflow/data/sales_extracted",
    header=True,
    inferSchema=True
)
df.show(5)

# Mengubah nama kolom menjadi snack_case agar lebih tersetruktur
def transform(df):
    '''
    Melakukan transformasi pada dataframe hasil extract data penjualan.

    Proses yang dilakukan:
    - Mengganti nama kolom agar lebih terstruktur dan mudah dibaca
    - Membuang informasi pecahan detik pada kolom 'time_details' 
    sehingga formatnya menjadi HH:mm:ss.
    - Merubah kolom 'date' menjadi tipe data date dengan 
    format yyyy-MM-dd.
    - Menghapus baris duplikat (jika ditemukan)
    
    '''
    df = (df
          .withColumnRenamed('cash_type', 'payment_type')
          .withColumnRenamed('money','total_price')
          .withColumnRenamed('Time_of_Day','day_part')
          .withColumnRenamed('Weekday','day_name')
          .withColumnRenamed('Month_name','month_name')
          .withColumnRenamed('Weekdaysort','number_day')
          .withColumnRenamed('Monthsort','number_month')
          .withColumnRenamed('Date','date')
          .withColumnRenamed('Time','time_details')
    )

    # Membuang informasi pecahan detik karena tidak dibutuhkan
    df = df.withColumn(
        "time_details",
        F.date_format(F.date_trunc("second", F.to_timestamp("time_details")), "HH:mm:ss")
    )
    
    # Memastikan kolomnya bertipe tahun-bulan-tanggal
    df = df.withColumn(
        "date",
        F.to_date(F.col("date"), "yyyy-MM-dd")
    )

    # Menghapus baris duplikat
    df = df.dropDuplicates()

    return df
if __name__=="__main__":
    # Membaca CSV hasil proses Extract
    df = spark.read.option("header",True).option("inferSchema",True).csv("/opt/airflow/data/sales_extracted")

    # Menjalankan proses transform
    df_transformed = transform(df)

    # Memeriksa hasil transformasi
    df_transformed.show(5)
    df_transformed.printSchema()

    # Menyimpan hasil transformasi ke CSV
    df_transformed.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/opt/airflow/data/sales_transformed")