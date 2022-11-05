import datetime
from airflow import models
from airflow.providers.google.cloud.operators import dataproc
from airflow.utils import trigger_rule


# Arguments to pass to Cloud Dataproc job.
PYSPARK_JOB = {
    "reference": {"project_id": '{{ var.value.gcp_project }}'},
    "placement": {"cluster_name": 'cluster-created-from-composer-{{ ds_nodash }}'},
    "pyspark_job": {
        "main_python_file_uri": 'gs://dataproc-gs/pyspark-job/main.py',
        "python_file_uris": ['gs://dataproc-gs/pyspark-job/src.zip'],
        "args": ["{{dag_run.conf['name']}}"]
    },
}

# Dataproc Cluster Configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2"
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2"
    },
}

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': '{{ var.value.gcp_project }}',
    'region': '{{ var.value.gce_region }}',

}


with models.DAG(
        'My_sample_DAG',
        # Continue to run DAG once per day
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    create_dataproc_cluster = dataproc.DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name='cluster-created-from-composer-{{ ds_nodash }}',
        cluster_config=CLUSTER_CONFIG,
        region='{{ var.value.gce_region }}'
    )

    # Runs Pyspark Job on the created cluster
    run_dataproc_hadoop = dataproc.DataprocSubmitJobOperator(
        task_id='run_dataproc_hadoop',
        job=PYSPARK_JOB
        )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='cluster-created-from-composer-{{ ds_nodash }}',
        region='{{ var.value.gce_region }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster

