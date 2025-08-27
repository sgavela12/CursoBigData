from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2025, 8, 26),
    schedule="* * * * *",  # Ejecutar cada minuto
    catchup=False
)
def dag_diario():
    
    @task
    def tarea_saludo():
        print("¡Buenos días! Ejecutando DAG diario")
        return "Tarea completada"

    @task
    def tarea_siguiente(resultado):
        print(f"Resultado anterior: {resultado}")

    # Dependencias
    resultado = tarea_saludo()
    tarea_siguiente(resultado)

dag = dag_diario()
