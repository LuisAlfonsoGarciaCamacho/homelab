from airflow.decorators import dag, task
from datetime import datetime


@dag(schedule=None, start_date=datetime(2026, 2, 1), catchup=False, tags=["test"])
def test_simple_dag_2():
    @task
    def get_value():
        return "hello from test 2"

    @task
    def print_value(msg: str):
        print(msg)
        return msg

    print_value(get_value())


test_simple_dag_2()
