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

print("🌡️ Mengirim data suhu ke Kafka... Tekan CTRL+C untuk berhenti.\n")

try:
    while True:
        for gudang_id in gudang_ids:
            suhu = random.randint(10, 90)  # Simulasi suhu antara 10–90 °C
            data = {"gudang_id": gudang_id, "suhu": suhu}
            producer.send('sensor-suhu-gudang', value=data)
            print(f"[KIRIM] {data}")
        time.sleep(1)  # Kirim setiap 1 detik
except KeyboardInterrupt:
    print("\n❌ Program dihentikan oleh pengguna.")
    producer.close()
