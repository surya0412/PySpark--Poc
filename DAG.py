import datetime
import os

from airflow import models
from airflow.providers.google.cloud.operators import dataproc
from airflow.utils import trigger_rule

# Output file for Cloud Dataproc job.
# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
output_file = os.path.join(
    '{{ var.value.gcs_bucket }}', 'wordcount',
    datetime.datetime.now().strftime('%Y%m%d-%H%M%S')) + os.sep
# Path to Hadoop wordcount example available on every Dataproc cluster.
WORDCOUNT_JAR = (
    'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
)
# Arguments to pass to Cloud Dataproc job.
input_file = 'gs://pub/shakespeare/rose.txt'
wordcount_args = ['wordcount', input_file, output_file]

PYSPARK_JOB = {
    "reference": {"project_id": '{{ var.value.gcp_project }}'},
    "placement": {"cluster_name": 'composer-hadoop-tutorial-cluster-{{ ds_nodash }}'},
    "pyspark_job": {
        "main_python_file_uri": 'gs://dataproc-gs/main.py',
        "python_file_uris": ['gs://dataproc-gs/src.zip']
    },
}

# JOB = {
#         "placement": {"cluster_name": 'composer-hadoop-tutorial-cluster-{{ ds_nodash }}'},
#         "pyspark_job": {"main_python_file_uri": "gs://{}/{}".format(gcs_bucket, spark_filename)},
#     }

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-1"
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-1"
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
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        cluster_config=CLUSTER_CONFIG,
        region='{{ var.value.gce_region }}'
    )

    run_dataproc_hadoop = dataproc.DataprocSubmitJobOperator(
        task_id='run_dataproc_hadoop',
        job=PYSPARK_JOB
        )

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-hadoop-tutorial-cluster-{{ ds_nodash }}',
        region='{{ var.value.gce_region }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    create_dataproc_cluster >> run_dataproc_hadoop >> delete_dataproc_cluster