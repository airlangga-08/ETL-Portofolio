[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/XwS47GYo)
# Milestones 3

_Milestones 3 ini dibuat guna mengevaluasi pembelajaran pada Hacktiv8 Comprehensive Data Analytics Program khususnya pada Phase 2._

---
## Assignment Objectives

Milestone 3 ini dibuat dengan tujuan sebagai berikut:

- Mampu menggunakan Apache Airflow.

- Mampu melakukan validasi data dengan menggunakan Great Expectations.

- Mampu mempersiapkan data untuk digunakan sebelum masuk ke database NoSQL.

- Mampu mengolah data menggunakan library PySpark.
---

## Assignment Problems

Sebagai Data Engineer, kamu diminta untuk membuat data pipeline automation.

---
## Dataset

### Ketentuan Dataset
1. Pilihlah dataset yang paling nyaman digunakan dalam mengerjakan Milestone 3. Adapun ketentuan dataset yang harus digunakan adalah :
   * Setidaknya terdapat minimal 10 column.
   * Setiap column terdiri dari :
     + Capital letter dan lower letter atau semua huruf merupakan capital letter.
     + Contoh : `Age`, `fullName`, `CITY`, `Education Level`.
   * Tidak diperbolehkan memilih dataset dimana nama column terdiri dari lower letter saja.
   * Terdapat campuran column berbentuk kategorikal dan numerikal. Sebisa mungkin jumlah kolom kategorikal dan numerikal seimbang, tidak ada yang terlalu mendominasi.

2. **Konsultasikan terlebih dahulu dataset yang hendak digunakan ke instruktur Phase. Jika disetujui, maka silakan dikerjakan. Jika tidak disetujui, maka cari dataset yang lain dan konsultasikan lagi mengenai dataset yang baru ini.**

3. Student tidak boleh menggunakan dataset yang sudah dipakai dalam sesi pembelajaran saat dikelas bersama instruktur atau dataset pada tugas-tugas terdahulu dari Phase 0 hingga Phase 2.

4. **Student dilarang untuk melakukan scraping dataset** karena dikhawatirkan proses pembuatan scraper dan proses scraping akan memakan waktu. Gunakan public dataset yang tersedia diberbagai macam situs Internet.

5. Anda dapat membayangkan bahwa Anda saat ini bekerja sebagai seorang Data Engineer disebuah perusahaan. Carilah dataset yang sekiranya merepresentasikan domain sebuah perusahaan seperti data mengenai : 
   - Product inventory.
   - Revenue & profit.
   - Kinerja pegawai.
   - Customer profile.
   - Keluhan produk/jasa.
   - dll.

### Data Sources
Student dapat memilih dataset dari salah satu repository dibawah ini. Popular open data repositories :

- [UC Irvine Machine Learning Repository](https://archive.ics.uci.edu/ml/index.php)
- [Kaggle datasets](https://www.kaggle.com/datasets)
- [Amazon’s AWS datasets](https://registry.opendata.aws/)

Meta portals :

- [Data Portals](http://dataportals.org/)
- [OpenDataMonitor](https://opendatamonitor.eu/frontend/web/index.php?r=dashboard%2Findex)
- [Quandl](https://www.quandl.com/)
- Sumber lain yang kredibel.

---

## Assignment Instructions
Buatlah sistem automasi data pipeline (ETL) dengan mengkombinasikan penggunaan PySpark, Airflow, dan MongoDB.

### A. Pra Automasi
1. Tentukan dataset yang hendak dipakai. Beri nama dataset ini dengan `P2M3_<nama-student>_data_raw.csv`. Contoh : `P2M3_raka_ardhi_data_raw.csv`.
2. Sebelum membuat sistem automasi, kamu perlu melakukan eksplorasi data dan validasi data di **notebook (.ipynb)**.
3. Pada tahapan ini, kamu boleh menggunakan Pandas (**NOTE:** perlu diperhatikan bahwa jika ukuran data cukup besar, penggunaan Pandas tidak lagi efisien dan perlu menggunakan PySpark, namun untuk kasus ini kamu bisa ambil sampel data saja).
4. Lakukan eksplorasi data sederhana untuk mengetahui kondisi data dan kesimpulan yang ingin didapatkan adalah langkah-langkah data cleaning dan processing yang nantinya akan dikembangkan di data pipeline.
5. Selanjutnya lakukan validasi data menggunakan Great Expectations. Adapun kriteria mengenai Expectation yang dipilih adalah :
   - Lakukan minimal 7 Expectations yang didalamnya harus ada Expectation untuk:
     + to be unique
     + to be between min_value and max_value
     + to be in set
     + to be in type list
     + 3 jenis Expectation yang berbeda yang tidak diajarkan pada lecture Data Ethics & Data Validation
   - Kamu diwajibkan untuk menerapkan 4 Expectation yang telah ditentukan dan 3 Expectation lain yang tidak diajarkan pada lecture dimana semua Expectation ini berbeda satu sama lain.
   - Ketujuh Expectation yang digunakan haruslah semuanya bernilai `success: true`.
   - Untuk Expectation `to be unique`, Kamu diizinkan untuk membuat sebuah column baru jika dirasa column yang ada didataset tidak ada yang unik. Column baru ini haruslah berasal dari column yang sudah ada. Kamu dapat membuat gabungan antara beberapa column yang sudah ada untuk column baru ini. Buatlah skenario fiksi mengenai kegunaan dari column ini sehingga jelas peruntukannya.
   - Setiap Expectation hanya boleh berada pada 1 cell yang berbeda-beda sehingga dapat dilihat mengenai hasilnya.
   - Simpan Python Notebook dengan nama `P2M3_<nama-student>.ipynb`. Contoh : `P2M3_raka_ardhi.ipynb`.
  
### B. Automasi Data Pipeline
Buat proses extract, transform, dan load dalam file python script yang terpisah.
#### 1. Extract
- Buat file `extract.py` untuk membaca data saja.
- Gunakan  PySpark untuk membaca data.
- Buat script load data dalam bentuk function untuk memudahkan dan outpunya berupa Spark Dataframe yang nantinya akan dibaca oleh file transform.py
- Contoh:
  ```python
  from pyspark.sql import SparkSession
  def load_data(file_path):
      spark = SparkSession.builder.getOrCreate()
      data = spark.read.csv(file_path, header=True, inferSchema=True)
      return data```

#### 2. Transform
- Buat file `transform.py` untuk memproses data yang telah diekstraksi.
- Tahapan data cleaning dan processing hanya dilakukan dengan PySpark dan tahapannya sesuai dengan kesimpulan yang didapatkan dari eksplorasi data yang sudah dilakukan di notebook.
- Pastikan hasil transformasi data siap digunakan untuk proses selanjutnya.
- Contoh:
  ```python
  def transform(data):
    data_dropped = data.drop("col1")
    return data_dropped
  ```

#### 3. Load
- Buat file `load.py` untuk menyimpan data ke database Mongodb
- Buat koneksi ke database Mongodb dengan pymongo
- Simpan Pyspark dataframe ke database Mongodb
- Cek di Mongodb atlas apakah database sudah tersimpan dan screenshot. Push screenshotnya ke repository dengan nama file `P2M3_<nama-student>_screenshot_mongo.jpg`. Contoh: `P2M3_raka_ardhi_screenshot_mongo.jpg`


### Workflow Orchestration
Lakukan automasi dengan membuat DAG dengan kriteria :
   - DAG berisi 3 node/task dibawah ini :
     + `Extract` : berisi script untuk menjalankan file extract.py dan fungsi di dalamnya yang bertujuan untuk load data.
     + `Transform` : berisi script untuk menjalankan file transform.py dan fungsi di dalamnya yang bertujuan untuk transformasi data.
     + `Load` : berisi script untuk menjalankan file load.py dan fungsi di dalamnya yang bertujuan untuk menyimpan data ke database MongoDB.
   - Penjadwalan dimulai pada tanggal 01 November 2024.
   - Penjadwalan dilakukan setiap hari Sabtu jam 09:10 AM hingga 09:30 AM dan dilakukan per 10 menit. Tabel dibawah ini adalah contoh hasil penjadwalannya :
     | No | Execution Time |
     | --- | --- |
     | 1 | Saturday, 2 November 2024 09:10 AM |
     | 2 | Saturday, 2 November 2024 09:20 AM |
     | 3 | Saturday, 2 November 2024 09:30 AM |
     | 4 | Saturday, 9 November 2024 09:10 AM |
     | 5 | Saturday, 9 November 2024 09:20 AM |
     | 6 | Saturday, 9 November 2024 09:30 AM |
     | 7 | Saturday, 16 November 2024 09:10 AM |
     | 8 | Saturday, 16 November 2024 09:20 AM |
     | 9 | ... |
   - Simpan DAG dengan nama `P2M3_raka_ardhi_DAG.py`.
   - Jalankan penjadwalan yang Anda buat. Lalu, screenshot mengenai graph DAG yang telah Anda jalankan dengan nama `P2M3_raka_ardhi_DAG_graph.jpg`. *([Ilustrasi graph DAG](https://i.sstatic.net/KgUwc.png))*

### C. Ketentuan Lain
1. Pada file Python, **wajib** memberikan keterangan atau pengenalan dengan menggunakan `comment` atau `docstring` yang berisikan : Judul tugas, Nama, Batch, dan penjelasan singkat tentang program yang dibuat, fitur-fitur. Contoh:
    ```py
    '''
    =================================================
    Milestone 3

    Nama  : Raka Ardhi
    Batch : CODA-001-RMT

    Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
    =================================================
    '''
    ```

2. Kamu diwajibkan menggunakan class/function untuk memisahkan bagian code agar flow dari code yang dibuat mudah diikuti. Berikan penjelasan pada setiap class/function yang dibuat dengan menggunakan docstring seperti : 
   ```py
   def get_data_from_postgresql(url, database, table):
     '''
     Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

     Parameters:
      url: string - lokasi PostgreSQL
      database: string - nama database dimana data disimpan
      table: string - nama table dimana data disimpan

     Return
      data: list of str - daftar data yang ada di database
        
     Contoh penggunaan:
     data = get_data_from_postgresql('localhost', 'db_phase2', 'table_m3')
     '''

     return data

   ```

---

## Assignment Submission

- Push Assignment yang telah Anda buat ke akun GitHub Classroom Anda masing-masing.
- Contoh bentuk repository :
  ```
  P2-M3/raka-ardhi
  |
  ├── P2M3_raka_ardhi_data_raw.csv
  ├── P2M3_raka_ardhi_DAG.py 
  ├── P2M3_raka_ardhi_DAG_graph.jpg
  ├── P2M3_raka_ardhi_DAG_graph.py
  ├── P2M3_raka_ardhi_GX.ipynb 
  └── README.md
  ```

---

## Assignment Rubrics

### Code Review

| Criteria | Meet Expectations | Points |
| --- | --- | --- |
| DAG | DAG yang digunakan dapat dijalankan tanpa error | 12 pts |
| Great Expectation | Mampu membuat 7 Expectations dengan 0 Error | 2 pts / Expectation |
| PySpark | Mampu membuat script data cleaning dan transformation dengan PySpark | 6 |
| Conclusion | Penarikan kesimpulan dari hasil eksplorasi data sederhana berupa langkah-langkah data cleaning dan transformation yang akan dilakukan  | 5 pts |
| Runs Perfectly | Kode berjalan tanpa ada error. Seluruh kode berfungsi dan dibuat dengan benar. | 5 pts |

### Readability

| Criteria | Meet Expectations | Points |
| --- | --- | --- |
| Tertata Dengan Baik | Semua baris kode terdokumentasi dengan baik dengan Markdown untuk penjelasan kode | 8 pts |

```
Kriteria tertata dengan baik diantaranya adalah: 

1. Tidak menyalin markdown dari tugas lain.
2. Import library rapih (terdapat dalam 1 cell dan tidak ada unused libs).
3. Terdapat komentar pada setiap baris kode.
4. Adanya pemisah yang jelas antar section, dll.
5. Tidak adanya typo.
```

---

```
Total Points : 50
```

---

## Notes

* **Deadline : P2W3D2 pukul 23:59 WIB.**

* **Keterlambatan pengumpulan tugas mengakibatkan skor Milestone 3 menjadi 0.**

* **Presentasi Milestone 2 akan dilaksanakan di P2W3D4, harap siapkan presentasinya.**
