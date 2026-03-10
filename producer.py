import json
import time
import yfinance as yf
from kafka import KafkaProducer

# Configuration du Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'finance-streaming'

# Fonction pour récupérer les données boursières
def get_stock_data(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period='1d')
    df = stock.history(period="1d", interval="1m")
    if df.empty:
        raise Exception(f"Aucune donnee recue pour {ticker}")
    if not data.empty:
        last_price = df['Close'].iloc[-1]
        return {
        "symbol": ticker,
        "price": round(float(last_price), 2),
        "timestamp": time.time()
    }
    return None

# Envoi des données boursières en continu (toutes les 2 secondes)
try: 
    while True:
        stock_data = get_stock_data('NVDA')
        if stock_data:
            producer.send(TOPIC_NAME, value=stock_data)
            print(f"Envoi data: {stock_data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Arret producer...")
finally:
    producer.close()    