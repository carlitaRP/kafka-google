from kafka import KafkaProducer
import json
import pandas as pd
import sys

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_data(url):
    df = pd.read_json(url, lines=True)  # ✅ Importante: Agregar `lines=True` porque los datos están en formato JSONL

    for _, row in df.head (6113).iterrows():
        dict_data = row.to_dict()
        producer.send('motorcycle-maxhp', value=dict_data)
        print(f"Sent: {dict_data}")
    producer.close()

if __name__ == "__main__":
    # URL del dataset
    url = sys.argv[1]
    send_data(url)


# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)

# def on_send_error(excp):
#     print('I am an errback', exc_info=excp)

# ## Modificar la URL 
# url='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/results/motorcycle_sales/part-00000-280f54d0-d2ad-4206-832b-8a212b1850eb-c000.json'

# import pandas as pd

# df = pd.read_json(url, orient='records', lines=True)

# for index, value in df.head(100).iterrows():
#     dict_data = dict(value)
#     producer.send('motorcycle', value=dict_data)
#     print(dict_data)

# producer.close()