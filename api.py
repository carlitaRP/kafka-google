from fastapi import FastAPI
import subprocess

app = FastAPI()

@app.post("/send-to-mongo")
def send_to_mongo(data: dict):
    url = data.get("url")
    if not url:
        return {"error": "No URL provided"}

    subprocess.Popen(["python", "kafka-producer-mongo.py", url])
    return {"message": f"Enviando datos de {url} a MongoDB"}

@app.post("/send-to-postgres")
def send_to_postgres(data: dict):
    url = data.get("url")
    if not url:
        return {"error": "No URL provided"}

    subprocess.Popen(["python", "kafka-producer-pg.py", url])
    return {"message": f"Enviando datos de {url} a PostgreSQL"}
