from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import mysql.connector

# Configuración de conexión a MySQL
MYSQL_CONFIG = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "1234",
    "database": "prueba_spark"
}

OUTPUT_CSV = "output/productos_export.csv"


@dag(
    dag_id="mysql_to_csv",
    start_date=datetime(2025, 8, 27),
    schedule="@daily",
    catchup=False,
    tags=["mysql", "export", "csv"]
)
def mysql_to_csv_pipeline():

    @task
    def extraer():
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        query = "SELECT id, producto, precio FROM ventas_detalladas;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df.to_dict(orient="records")

    @task
    def transformar(registros):
        df = pd.DataFrame(registros)
        # nombres en mayúsculas
        df["producto"] = df["producto"].str.upper()
        return df.to_dict(orient="records")

    @task
    def cargar(registros):
        df = pd.DataFrame(registros)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ CSV generado en: {OUTPUT_CSV}")

    # Flujo de dependencias
    datos = extraer()
    datos_limpios = transformar(datos)
    cargar(datos_limpios)


mysql_to_csv_pipeline = mysql_to_csv_pipeline()
