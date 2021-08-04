from airflow import DAG
from airflow.models import baseoperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from random import randint

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy>9):
        return 'accurate'
    elif (best_accuracy==5):
        return 'value_5'
    return 'inaccurate'

def _training_model():
    return randint(1,10)

def _show_5(ti):
    return print(accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ]))

with DAG("my_dag", start_date=datetime(2021,1,1,11,32,1),
        schedule_interval=timedelta(seconds=10), catchup=False) as dag:
            training_model_A = PythonOperator(
                task_id="training_model_A",
                python_callable=_training_model
            )
            training_model_B = PythonOperator(
                task_id="training_model_B",
                python_callable=_training_model
            )
            training_model_C = PythonOperator(
                task_id="training_model_C",
                python_callable=_training_model
            )

            choose_best_model =BranchPythonOperator(
                task_id="choose_best_model",
                python_callable=_choose_best_model
            )

            accurate = BashOperator(
                task_id="accurate",
                bash_command="echo 'accurate'"
            )

            inaccurate = BashOperator(
                task_id="inaccurate",
                bash_command="echo 'inaccurate'"
            )
            value_5 = BashOperator(
                task_id='value_5',
                bash_command="echo 'value_5'"
            )
            [training_model_A,training_model_B,training_model_C] >> choose_best_model >> [accurate,value_5,inaccurate]