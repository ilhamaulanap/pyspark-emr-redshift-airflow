from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

from datetime import datetime, timedelta


BUCKET_NAME = "project-pyspark-airflow"
s3_data = "dataset/moviesdataset/"
s3_script = "scripts/"
s3_jars = "jars/"
s3_clean = "clean-data/"

default_args = {
    "owner" : 'Ilham Putra',
    "start_date" : datetime.today(),
    "depends_on_past" : False,
    "email_on_failure" : False,
    "retries": 3,
    "retry_delay":timedelta(minutes=1)
}

dag = DAG("Pyspark-emr-airflow", default_args = default_args, 
          description = "Pyspark on EMR with Airflow and load to redshift",
          schedule_interval = "@once")

SPARK_STEPS = [
    {
        "Name": "Spark script files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT", 
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "Data files from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_data}}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "JAR file from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.BUCKET_NAME }}/{{ params.s3_jars}}",
                "--dest=/usr/lib/spark/jars/",
            ],
        },
    },
    {
        "Name": "Genre",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jar",
                "/usr/lib/spark/jars/redshift-jdbc42-2.1.0.21.jar",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/scripts/genre.py",
            ],
        },
    },
    {
        "Name": "Date",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jar",
                "/usr/lib/spark/jars/redshift-jdbc42-2.1.0.21.jar",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/scripts/date.py",
            ],
        },
    },
    {
        "Name": "Rating",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jar",
                "/usr/lib/spark/jars/redshift-jdbc42-2.1.0.21.jar",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/scripts/rating.py",
            ],
        },
    },
    {
        "Name": "Movie",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jar",
                "/usr/lib/spark/jars/redshift-jdbc42-2.1.0.21.jar",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/scripts/movie.py",
            ],
        },
    },
    {
        "Name": "Budget&Revenue",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--jar",
                "/usr/lib/spark/jars/redshift-jdbc42-2.1.0.21.jar",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/scripts/budgetrevenue.py",
            ],
        },
    },
    {
        "Name": "Move clean data from HDFS to S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_clean }}",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "pysparkairflow_Project",
    "ReleaseLabel": "emr-5.28.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3",  "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"},
                },
            ],

        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://project-pyspark-airflow/job",
    "VisibleToAllUsers": True
}

#Start pipeline
start_operator = DummyOperator(task_id='begin-execution', dag=dag)

#Create necessary schema and tables for Data Warehouse in Redshift
create_tables =RedshiftDataOperator(
    task_id = 'Create_Tables_Redshift',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/create_tables.sql", dag=dag)


#Create EMR Cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag
)

# Add the steps once the cluster is up 
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
        "jar_files": s3_jars
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# temrinate the cluster once steps/tasks are completed 
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

stage_rating_to_dwh =RedshiftDataOperator(
    task_id = 'stage_rating_to_dwh',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/rating.sql", dag=dag)

stage_movies_to_dwh =RedshiftDataOperator(
    task_id = 'stage_movies_to_dwh',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/movies.sql", dag=dag)


stage_genre_to_dwh =RedshiftDataOperator(
    task_id = 'stage_genre_to_dwh',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/genre.sql", dag=dag)

stage_date_to_dwh =RedshiftDataOperator(
    task_id = 'stage_date_to_dwh',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/date.sql", dag=dag)

stage_budgetrevenue_to_dwh =RedshiftDataOperator(
    task_id = 'stage_budgetrevenue_to_dwh',
    database="dev",
    cluster_identifier="aws-redshift-cluster-1",
    aws_conn_id="aws_default",
    region = "ap-southeast-1",
    sql="scripts/sql/budgetrevenue.sql", dag=dag)


# end pipeline
end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)



start_operator >> create_tables
create_tables >> create_emr_cluster >> step_adder >> step_checker >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> [stage_rating_to_dwh, stage_movies_to_dwh, stage_genre_to_dwh, stage_date_to_dwh, stage_budgetrevenue_to_dwh]
[stage_rating_to_dwh, stage_movies_to_dwh, stage_genre_to_dwh, stage_date_to_dwh, stage_budgetrevenue_to_dwh] >> end_data_pipeline