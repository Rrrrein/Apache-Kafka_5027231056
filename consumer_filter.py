from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema untuk masing-masing sensor
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# 3. Baca stream dari topik suhu
df_suhu = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

df_suhu_parsed = df_suhu.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*") \
    .filter(col("suhu") > 80)

# 4. Baca stream dari topik kelembaban
df_kelembaban = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

df_kelembaban_parsed = df_kelembaban.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*") \
    .filter(col("kelembaban") > 70)

# 5. Fungsi cetak custom
def tampilkan_peringatan_suhu(df, epoch_id):
    for row in df.collect():
        print(f"[🔥 Peringatan Suhu Tinggi] Gudang {row['gudang_id']}: Suhu {row['suhu']}°C")

def tampilkan_peringatan_kelembaban(df, epoch_id):
    for row in df.collect():
        print(f"[💧 Peringatan Kelembaban Tinggi] Gudang {row['gudang_id']}: Kelembaban {row['kelembaban']}%")

# 6. Jalankan stream untuk suhu
query_suhu = df_suhu_parsed.writeStream \
    .foreachBatch(tampilkan_peringatan_suhu) \
    .outputMode("append") \
    .start()

# 7. Jalankan stream untuk kelembaban
query_kelembaban = df_kelembaban_parsed.writeStream \
    .foreachBatch(tampilkan_peringatan_kelembaban) \
    .outputMode("append") \
    .start()

# 8. Tunggu hingga streaming selesai
query_suhu.awaitTermination()
query_kelembaban.awaitTermination()
