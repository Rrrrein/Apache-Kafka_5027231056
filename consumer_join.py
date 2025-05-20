from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# 1. Setup Spark
spark = SparkSession.builder \
    .appName("JoinSensorStreams") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Schema untuk kedua sensor
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# 3. Read stream suhu + timestamp
df_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

suhu_stream = df_suhu.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_suhu).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp")

# 4. Read stream kelembaban + timestamp
df_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

kelembaban_stream = df_kelembaban.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_kelembaban).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp")

# 5. Windowed join berdasarkan gudang_id dan waktu 10 detik
suhu_windowed = suhu_stream.withWatermark("timestamp", "15 seconds") \
    .groupBy(window("timestamp", "10 seconds"), "gudang_id") \
    .agg(expr("max(suhu) as suhu"))

kelembaban_windowed = kelembaban_stream.withWatermark("timestamp", "15 seconds") \
    .groupBy(window("timestamp", "10 seconds"), "gudang_id") \
    .agg(expr("max(kelembaban) as kelembaban"))

joined = suhu_windowed.join(
    kelembaban_windowed,
    on=["window", "gudang_id"]
)

# 6. Filtering untuk deteksi kritis
peringatan_kritis = joined.filter(
    (col("suhu") > 80) & (col("kelembaban") > 70)
)

# 7. Custom print
def tampilkan_peringatan_kritis(df, epoch_id):
    for row in df.collect():
        print(f"[🚨 PERINGATAN KRITIS] Gudang {row['gudang_id']}: Suhu {row['suhu']}°C, Kelembaban {row['kelembaban']}% — Barang berisiko rusak!")

# 8. Streaming output
query = peringatan_kritis.writeStream \
    .foreachBatch(tampilkan_peringatan_kritis) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/cekpoin_kritis") \
    .start()

query.awaitTermination()
