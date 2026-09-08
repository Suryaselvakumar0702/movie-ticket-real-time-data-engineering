from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="movie_ticket_pipeline",
    start_date=datetime(2026, 9, 6),
    schedule="@daily",
    catchup=False,
    tags=["movie", "data-engineering"],
) as dag:

    load_movies = BashOperator(
        task_id="load_movies",
        bash_command="python /opt/project/load_movie.py",
    )

    validate_movies = BashOperator(
        task_id="validate_movies",
        bash_command="python /opt/project/validate_movies.py",
    )

    load_movies >> validate_movies