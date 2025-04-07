from kafka import KafkaConsumer
import json
import psycopg2

print('connecting pg ...')

try:
    conn = psycopg2.connect(database = "defaultdb", 
                        user = "avnadmin", 
                        host= 'pg-359cfab6-estudiantes-bb9b.h.aivencloud.com',
                        password = "AVNS_-OKTlUSsFpRD_i04C5-",
                        port = 27234)
    cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")

consumer = KafkaConsumer('motorcycle-maxhp',bootstrap_servers=['kafka:9092'])

for msg in consumer:
    record = json.loads(msg.value.decode('utf-8'))  
    brand = record["Brand"]  
    model = record["Model"]
    power_hp = float(record["Power (hp)"])  
    displacement_ccm = float(record["Displacement (ccm)"])  

    try:
        sql = """
        INSERT INTO motorcycle (brand, model, power_hp, displacement_ccm)
        VALUES (%s, %s, %s, %s)
        """  
        cur.execute(sql, (brand, model, power_hp, displacement_ccm))  
        conn.commit()
        print(f"Inserted: {brand} - {model} - {power_hp} HP - {displacement_ccm} ccm")
    except Exception as e:
        print("Could not insert into PostgreSQL:", e)

conn.close()