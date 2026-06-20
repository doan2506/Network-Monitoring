import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BROKER = 'localhost:29092' 
KAFKA_TOPIC = 'network_alerts'

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env file")

def send_telegram_alert(data):
    if not data: return
    
    text = f"🚨 *NETWORK ATTACK DETECTED (BATCH)* 🚨\n\n"
    text += f"• *Total Flows:* {data.get('total_flows')}\n"
    text += f"• *Attacks Detected:* {data.get('attack_count')}\n"
    text += f"• *Max Fwd Pkt Len:* {data.get('fwd_pkt_len_max'):.2f} Bytes\n"
    text += f"• *Avg Duration:* {data.get('flow_duration'):.2f} ms\n"
    text += f"• *Flow Packets/s:* {data.get('flow_packets_s'):.2f}\n"
    text += f"• *Flow Bytes/s:* {data.get('flow_bytes_s'):.2f}\n"
    text += f"• *SYN Flags:* {data.get('syn_flags')}\n"
    text += f"• *RST Flags:* {data.get('rst_flags')}\n"
    text += f"• *ACK Flags:* {data.get('ack_flags')}\n"
    text += f"• *FIN Flags:* {data.get('fin_flags')}\n"
    text += f"• *PSH Flags:* {data.get('psh_flags')}\n"
    text += f"\n_Check Grafana for more details_"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code == 200:
            print(f"[{time.strftime('%X')}] Successfully sent batch alert to Telegram.")
        else:
            print(f"Failed to send Telegram alert: {response.text}")
    except Exception as e:
        print(f"Telegram connection error: {e}")

if __name__ == "__main__":
    print(f"Started listening for alerts from Kafka: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC}...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', 
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        metrics_data = message.value
        send_telegram_alert(metrics_data)
