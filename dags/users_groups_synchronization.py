import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.models import Variable
import os
from tasks.synchronize_groups import syn_dp_ad_groups
from tasks.synchronize_users import syn_dp_ad_users


#Initialize default graph args

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2018, 8, 1),
    'retries': 0,
    'depends_on_past': False,
    'retry_delay': datetime.timedelta(minutes=5),
    'schedule_interval' : '@daily',
    'max_active_runs':1
}

# Create graph
dag_pr_sync_ad_dp_users_groups = DAG(dag_id='synchronize_dp_ad_users_groups',
                      description='Synchronize data portail  and Azure AD users and groups',
                      default_args=default_args,
                      catchup=False)


aws_access_key_id=Variable.get('sync_aws_access_key')
aws_secret_access_key = Variable.get('sync_aws_secret_key')
region_name = Variable.get('sync_region_name')


azure_client_id = Variable.get('client_id')
azure_client_secret= Variable.get('sync_azure_client_secret')
azure_tenant=Variable.get('sync_azure_tenant')
parameters = Variable.get("sync_users_groups_params",deserialize_json=True)
graph_api_paramrters = parameters.get("graph_api_params")


task_sync_groups = PythonVirtualenvOperator(
    task_id='task_synchronize_groups',
    python_callable=syn_dp_ad_groups,
    op_kwargs={
        'aws_access_key_id':aws_access_key_id,
        'aws_secret_access_key':aws_secret_access_key,
        'region_name':region_name,
        'azure_client_id':azure_client_id,
        'azure_client_secret':azure_client_secret,
        'azure_tenant':azure_tenant,
        'params' : parameters,
        'script_path':os.path.dirname(__file__)

    },
    requirements=['azure-graphrbac','boto3','pandas'],
    dag=dag_pr_sync_ad_dp_users_groups,
    email_on_failure=True,
    email='mehdi.hadria@neoxia.com'
)

task_sync_users = PythonVirtualenvOperator(
    task_id='task_synchronize_users',
    python_callable=syn_dp_ad_users,
    op_kwargs={
        'aws_access_key_id':aws_access_key_id,
        'aws_secret_access_key':aws_secret_access_key,
        'region_name':region_name,
        'azure_client_id':azure_client_id,
        'azure_client_secret':azure_client_secret,
        'azure_tenant':azure_tenant,
        'params' : parameters,
        'script_path':os.path.dirname(__file__)

    },
    requirements=['azure-graphrbac','boto3','pandas'],
    dag=dag_pr_sync_ad_dp_users_groups,
    email_on_failure=True,
    email='mehdi.hadria@neoxia.com'
)

task_sync_groups >> task_sync_users



syn_dp_ad_users(
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
        azure_client_id,
        azure_client_secret,
        azure_tenant,
        parameters,
        ''
        )






