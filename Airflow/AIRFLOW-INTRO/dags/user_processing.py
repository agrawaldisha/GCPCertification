from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
# from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests

#ti is reserved keyword in airflow
#xcom is machanism to share an arbitary task in airflow, stored in metadata database 

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

    

    @task
    def extract_user(fake_user):
        return {
            "id":fake_user["id"],
            "firstname":fake_user["personalInfo"]["firstName"],
            "lastName":fake_user["personalInfo"]["lastName"],
            "email":fake_user["personalInfo"]["email"],
            
        }
        print(fake_user)
    
    @task
    def process_user(user_info):
        import csv  #alows to manipulate csv file
        # user_info={'id': '1293234', 'firstname': 'John', 'lastName': 'Doe', 'email': 'johndoe@example.com'}
        with open("/tmp/user_info.csv", mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)
    # create_table >> api_sensor >>extract_user >> process_user # added task dependency

   


dag = user_processing()
