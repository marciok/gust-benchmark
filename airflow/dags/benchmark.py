import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="test_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def test_dag():
    @task
    def hello():
        print("hello from airflow")

    hello()


test_dag()
