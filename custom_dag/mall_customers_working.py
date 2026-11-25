from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task, get_current_context
import pendulum
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id = "simple_dag_for_loading_data_calc_and_add_some_new_values",
    schedule = pendulum.duration(minutes = 10),
    start_date = pendulum.now(),
    tags = ["test_dag"]
)
def simple_workflow_load():
    def check_file(ctx):
        return Path('/opt/airflow/dags/data_mall/Mall_Customers.csv').exists() 
    
    @task.skip_if(check_file)
    @task.bash(task_id = 'load')
    def load():
        ctx = get_current_context()
        logger.info(ctx)
        return 'cd /opt/airflow/dags && curl -L -o /opt/airflow/dags/data_mall/Mall_Customers.csv "https://raw.githubusercontent.com/gakudo-ai/open-datasets/main/Mall_Customers.csv"'
    
    operator_spark = SparkSubmitOperator(
        application = '/opt/airflow/spark/spark_pipe.py',
        name='test_spark_spark',
        task_id = 'test_task_spark',
        conn_id = 'spark_local', 
        trigger_rule = 'none_failed',
        total_executor_cores=2
    )    

    load() >> operator_spark
    
simple_workflow_load()
