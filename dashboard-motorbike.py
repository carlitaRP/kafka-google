import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

# Inicializa conexión a Mongo.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# Extraer datos del cvs
# Utiliza st.cache_data para volver a ejecutar 
# solo cuando cambia la consulta o cada 10 minutos.
@st.cache_data(ttl=600)
def get_data():
    db = client.motorcycle
    items = db.motorcycle.find()
    items = list(items)  # make hashable for st.cache_data
    return items

# Inicializa conexión a PostgreSQL.
conn = st.connection("postgresql", type="sql")

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
        "event_type": job,
        "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
    }

    }

    headers = {
    'Authorization': 'Bearer ' + token,
    'Accept': 'application/vnd.github.v3+json',
    'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    st.write(response)

    # Display the response in the app

# Sidebar con información personal
sidebar = st.sidebar
sidebar.markdown("<h2 style='text-align: center; color: #FF4C4C;'>🌼Carlitarp🌼</h2>", unsafe_allow_html=True)
sidebar.image('carlita.jpg', width=160)
sidebar.markdown("<h4 style='text-align: center; color: #CCCCCC;'>🧡S20006731 ISW🧡</h4>", unsafe_allow_html=True)
sidebar.markdown("### 📩 Contacto")
sidebar.markdown("[✉️ zS20006731@estudiantes.uv.mx](mailto:zS20006731@estudiantes.uv.mx)")
sidebar.markdown("______")

# 🔥 Título Principal con estilo deportivo
st.markdown("<h1 style='text-align: center; color: #FF4C4C;'>🔥 Catálogo de Motos 🔥</h1>", unsafe_allow_html=True)
st.subheader("🏁 **Velocidad, potencia y rendimiento en un solo lugar**")

github_user  =  st.text_input('Github user', value='carlitaRP')
github_repo  =  st.text_input('Github repo', value='kafka')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='')
code_url     =  st.text_input('Code URL', value='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/motorbike.py')
dataset_url  =  st.text_input('Dataset URL', value='https://raw.githubusercontent.com/carlitaRP/kafka/refs/heads/main/all_bike.csv')

if st.button("🚀 **Envía tu trabajo a Spark**"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)
st.markdown("______")

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if response.status_code == 200:
        try:
            json_objects = [json.loads(line) for line in response.text.strip().split("\n")]
            st.write(json_objects)  # Muestra la lista de diccionarios
        except json.JSONDecodeError as e:
            st.error(f"Error al decodificar JSON: {e}")

st.header("🏁 **Consulta resultados de Spark**")

url_results=  st.text_input('URL results', value='')

if st.button("🚀 **Consulta resultados**"):
    get_spark_results(url_results)
st.markdown("______")

##API_URL = "http://api:8000"
API_URL = "http://34.41.218.148:8000"

st.title("🏁 **Envio de informacion a MongoDB y PostgreSQL**")

dataset_url_mg = st.text_input("URL de datos a guardar en MongoDB", value='')
if st.button("Enviar datos a MongoDB"):
    if dataset_url_mg:
        response = requests.post(f"{API_URL}/send-to-mongo", json={"url": dataset_url_mg})
        st.success(f"Respuesta: {response.json()}")
    else:
        st.error("Por favor, ingresa una URL válida.")

dataset_url_pg = st.text_input("URL de datos a guardar en PostgreSQL", value='')
if st.button("Enviar datos a PostgreSQL"):
    if dataset_url_pg:
        response = requests.post(f"{API_URL}/send-to-postgres", json={"url": dataset_url_pg})
        st.success(f"Respuesta: {response.json()}")
    else:
        st.error("Por favor, ingresa una URL válida.")
st.markdown("____") 

st.sidebar.markdown("### 🏁 **Consulta base de datos**")
if st.sidebar.button("Obtener resultados de MongoBD"):
    st.header("🏁 **Consulta base de datos MongoDB**")
    items = get_data()

    # Convertimos los resultados en una lista y los ordenamos por marca
    items_list = sorted(items, key=lambda x: x.get("Brand", "Desconocido"))

    # Agrupar por marca
    grouped_by_brand = {}
    for item in items_list:
        brand = item.get("Brand", "Desconocido")
        if brand not in grouped_by_brand:
            grouped_by_brand[brand] = []
        grouped_by_brand[brand].append(item)

    # Mostrar los resultados por marca
    for brand, models in grouped_by_brand.items():
        with st.expander(f"🔹 {brand} ({len(models)} modelos)"):
            for item in models:
                model = item.get("Model", "Desconocido")
                st.write(f"**Modelo:** {model}")


    st.markdown("______")

## Banco de imagenes para que se vea mas perron el trabajo jajaja
motorcycle_images = {
    "r 1200 gs": "https://mediapool.bmwgroup.com/cache/P9/201609/P90235547/P90235547-the-new-bmw-r-1200-gs-11-2016-600px.jpg",
    "r 1200 gs adventure": "https://www.mundomotero.com/wp-content/uploads/2017/07/BMW-R-1200-GS-Adventure-2018-7-1024x768.jpg",
    
}

## Metodo para obtener la imagen de la moto
def get_motorcycle_image(model):
    # Buscar la imagen en el diccionario
    return motorcycle_images.get(model.lower(), "https://via.placeholder.com/300") 

if st.sidebar.button("Obtener resultados de PostgreSQL"):
    st.header("🏁 **Consulta base de datos PostgreSQL**")
    
    # Ejecutar la consulta en PostgreSQL
    # df = conn.query('SELECT * FROM motorcycle;', ttl="10m")
    df = conn.query('SELECT * FROM motorcycle;')
    
    # Agrupar los resultados por marca
    grouped_by_brand = {}
    for row in df.itertuples():
        brand = row.brand  # Obtener la marca de cada fila
        if brand not in grouped_by_brand:
            grouped_by_brand[brand] = []
        # Añadir el modelo a la lista de esa marca
        grouped_by_brand[brand].append({
            "model": row.model,
            "power_hp": row.power_hp,
            "displacement_ccm": row.displacement_ccm
        })
    
    # Mostrar los resultados por marca
    for brand, models in grouped_by_brand.items():
        with st.expander(f"🔹 {brand} ({len(models)} modelos)"):
            for model in models:
                # Obtener la imagen para el modelo usando los enlaces directos
                image_url = get_motorcycle_image(model['model'])
                
                # Mostrar la imagen
                st.image(image_url, caption=model['model'], use_container_width=True)

                st.write(f"**Modelo:** {model['model']}")
                st.write(f"**Potencia (hp):** {model['power_hp']}")
                st.write(f"**Cilindrada (ccm):** {model['displacement_ccm']}")
                st.markdown("______")
st.sidebar.markdown("______")
