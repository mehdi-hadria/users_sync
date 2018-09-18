from utils.azure_graph_utils import generate_token, get_api_all_pages_items, get_users_recursively,get_api_page_items
import sys
import pandas as pd
import numpy as np
import datetime
import logging
from airflow.models import Variable
import pickle
from utils.dynamodb_utils import push_ad_data, get_dp_data, get_dynamodb_table



a = set({'dc28df5b-b376-4bb5-84ef-18c9b2254fd7', '00969ddb-0da4-494e-be29-2ea8c4c58657', 'd6ec2b03-11d0-47a8-b555-0aae9098d462', '88b303f0-4e70-459f-8075-d1b46f98aac2', 'e50dd091-c99a-4731-b765-d76d7019e153', '9edb877c-b578-4e8e-8f12-8b2dd6838de3', '1fe14022-063c-4515-b69c-62c574081979', 'cb4abe9f-3f08-4065-a3a3-0ea5c2c757e3', '69b2fd77-c209-4ba3-87c9-40113ef37c1e', '23a0a630-8a3e-4630-bbf1-32bedc95c96b', 'e5de9838-75e2-4684-a67d-4f7efca9c6a3', 'b38c22d5-b98f-493f-96dc-2e48d14dd80c', '63bc915f-76b5-4797-9576-5d154456c281', '7cf3dde4-0109-4b9f-aa69-91c8ef5232a2', 'db05557f-4acd-4a0d-885a-8989ed15f1b5'})
b = set({'342c5741-0a04-4912-b984-86294ca21171', '6f7ef188-22f3-448b-a955-0f9485b57c6d', 'c0808c95-30f5-43ed-806c-2e0b5bca209a', '7cef639d-180a-4b7b-9ee4-237529a5e489', '6af35ff8-ff13-45af-aa77-19fc3758f0e7', '093f8a16-0d8f-4db0-92ea-149086dfdcd1', '6e07131b-14d4-4bd7-9421-df0d769cd9cc', 'a4072084-4d19-4404-aff3-c935f276130d', '6fb3610e-b9cf-4725-8fe1-d8468de2189e', '01168e95-f0b9-41b5-9e3a-bf74b8a7df44'})
if a== b:
    print('ok')
'''
aws_access_key_id=Variable.get('sync_aws_access_key')
aws_secret_access_key = Variable.get('sync_aws_secret_key')
region_name = Variable.get('sync_region_name')


azure_client_id = Variable.get('client_id')
azure_client_secret= Variable.get('sync_azure_client_secret')
azure_tenant=Variable.get('sync_azure_tenant')
parameters = Variable.get("sync_users_groups_params",deserialize_json=True)
graph_api_paramrters = parameters.get("graph_api_params")


df_users_ad = pd.read_pickle('/Users/hadria/Documents/users/ad_users.pkl')
df_users_dp = pd.read_pickle('/Users/hadria/Documents/users/dp_users.pkl')
df_existing_users = pd.read_pickle('/Users/hadria/Documents/users/existing_users.pkl')


print(df_users_dp.columns)
print(df_users_ad.columns)
print(df_existing_users.columns)
df_existing_users['ad_groups'] = df_existing_users.apply(lambda row: row['groups'].difference(['user_native_groups']),axis=1)
dp_groups = get_dp_data(aws_access_key_id,
                        aws_secret_access_key,
                        region_name,
                        parameters['groups_table'],
                        filters_list=["#tp = :t", "attribute_not_exists(policies)"],
                        attributes_list=['group_key'],
                        attribute_names={"#tp": "type"},
                        attribute_values={":t": "native"})
list_dp_groups = [grp['group_key'] for grp in dp_groups]
df_users_dp.to_csv('/Users/hadria/Documents/users/dp_groups.csv')

df_exst = pd.merge(df_users_ad, df_users_dp, left_on='user_id',
                             right_on='user_id')

print(df_exst.columns)

print(df_exst[['user_id','groups_x','groups_y']][(df_exst.groups_x != df_exst.groups_y) & (df_exst.user_id ==' adekunle.abudu@pernod-ricard.com')].to_csv('/Users/hadria/Documents/users/adekunle.abudu@pernod-ricard.csv'))

print()
df_existing_users[['groups','user_native_groups','updated_groups']][df_existing_users.user_id=='adekunle.abudu@pernod-ricard.com'].to_csv('/Users/hadria/Documents/users/with_all_groups.csv')

'''