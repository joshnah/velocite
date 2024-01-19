#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses SparkKubernetesOperator and SparkKubernetesSensor.
In this example, we create two tasks which execute sequentially.
The first task is to submit sparkApplication on Kubernetes cluster(the example uses spark-pi application).
and the second task is to check the final state of the sparkApplication that submitted in the first state.

Spark-on-k8s operator is required to be already installed on Kubernetes
https://github.com/GoogleCloudPlatform/spark-on-k8s-operator
"""
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import timedelta
# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import timedelta 
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s_models

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,

}

dag = DAG(
    'fetch_stations',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    tags=['fetch'],
    concurrency=1,
    catchup=False
)

submit = KubernetesPodOperator(
    image="registry.gitlab.com/viviane.qian/projet-sdtd/producer:latest",
    namespace="messaging",
    name="fetch_stations",
    task_id="fetch_stations",
    image_pull_policy="Always",
    image_pull_secrets=[k8s.V1LocalObjectReference('registry-credentials')],
    dag=dag,
    configmaps=['producer-configmap'],
    get_logs = True,
    in_cluster=True,
    on_finish_action='delete_pod',
    container_resources=k8s_models.V1ResourceRequirements(
            limits={"memory": "512M", "cpu": "200m"},
        ),
    cmds=["python", "fetch_stations.py"]

)

submit
