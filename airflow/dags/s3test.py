from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago



with DAG(
    dag_id='s3cli_test_dag',
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:


    bash_hello = BashOperator(
        task_id='print_hello_world',
        bash_command='aws --endpoint-url https://10.0.1.42:9000 --no-verify-ssl s3 ls isd-weather --profile aws_sedev1_df --human-readable'
    )

bash_hello
