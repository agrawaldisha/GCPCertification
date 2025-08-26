from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
# from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

#ti is reserved keyword in airflow
#xcom is machanism to share an arbitary task in airflow, stored in metadata database 

def _extract_user(ti):
    # fake_user=ti.xcom_pull(task_ids="is_api_available")
    response = requests.get(
            "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
        )
    fake_user=response.json()
    return {
        "id":fake_user["id"],
        "firstname":fake_user["personalInfo"]["firstName"],
        "lastName":fake_user["personalInfo"]["lastName"],
        "email":fake_user["personalInfo"]["email"],
        
    }
    print(fake_user)

@dag(
    dag_id="user_processing",
    start_date=datetime(2025, 8, 1),
    schedule=None,   # fixed param
    catchup=False,
    tags=["example"],
)
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",   # must exist in Airflow Connections
        sql="""
            CREATE TABLE IF NOT EXISTS users(
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        response = requests.get(
            "https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
        )
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)

    api_sensor = is_api_available()

    extract_user=PythonOperator(
        task_id="extract_user",
        python_callable=_extract_user
    )

    create_table >> api_sensor >>extract_user  # added task dependency

   


dag = user_processing()
