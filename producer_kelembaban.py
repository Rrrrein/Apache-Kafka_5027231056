from kafka import KafkaProducer
import json
import time
import random

# Konfigurasi Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Daftar gudang
gudang_ids = ['G1', 'G2', 'G3']

print("💧 Mengirim data kelembaban ke Kafka... Tekan CTRL+C untuk berhenti.\n")

try:
    while True:
        for gudang_id in gudang_ids:
            kelembaban = random.randint(30, 80)  # Simulasi kelembaban antara 30–80%
            data = {"gudang_id": gudang_id, "kelembaban": kelembaban}
            producer.send('sensor-kelembaban-gudang', value=data)
            print(f"[KIRIM] {data}")
        time.sleep(1)  # Kirim setiap 1 detik
except KeyboardInterrupt:
    print("\n❌ Program dihentikan oleh pengguna.")
    producer.close()
