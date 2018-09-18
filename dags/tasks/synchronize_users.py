# Users from AZURE AD
def syn_dp_ad_users(
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
        azure_client_id,
        azure_client_secret,
        azure_tenant,
        params,
        script_path
        ):

    import sys
    import pandas as pd
    import numpy as np
    import datetime
    import logging


    sys.path.insert(0,script_path)
    from tasks.utils.dynamodb_utils import push_ad_data, get_dp_data,get_dynamodb_table
    from tasks.utils.azure_graph_utils import generate_token, get_api_all_pages_items, get_users_recursively

    def get_azure_ad_users(
            azure_client_id,
            azure_client_secret,
            azure_tenant,
            params):
        # Parameters for azure authentication  :
        enriched_params = dict(params['graph_api_params'])
        enriched_params['$expand'] = 'memberOf'
        token = generate_token(azure_client_secret, azure_client_id)
        header = {"Authorization": token}
        users_ad_list = get_api_all_pages_items('users', enriched_params, header, azure_tenant)
        # List to store users
        extracted_ad_users = []
        # Getting users
        users_in_scope_group = get_users_recursively(params['scope_group_id'], header, azure_tenant,
                                                     params['graph_api_params'])
        users = [user for user in users_ad_list if
                 user['objectId'] in users_in_scope_group and user['mail'] is not None]
        for user in users:
            if user.get('mail') is not None:
                new_user = {
                    'user_id': user.get('mail').lower(),
                    'user_type': 'employee',
                    'name': user.get('displayName', None),
                    'groups': set([grp['objectId'] for grp in user['memberOf']]),
                    'active': 'NO',  # 'Yes' if user.get('account_enabled',None)==True  else 'No',
                    'name_to_search': user.get('displayName', '').lower(),
                    'ad_information': {

                        'companyName': user.get('companyName', None),
                        'country': user.get('country', None),
                        'displayName': user.get('displayName', None),
                        'id': user.get('objectId', None),
                        'jobTitle': user.get('jobTitle', None),
                        'mail': user.get('mail', '').lower(),
                        'officeLocation': user.get('physicalDeliveryOfficeName', None),
                        'postalCode': user.get('postalCode', None),

                    },
                    'createdAt': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                }

            extracted_ad_users.append(new_user)
        return extracted_ad_users


    logging.info('Step 1 Getting users from Azure AD')
    df_users_ad = pd.DataFrame(get_azure_ad_users(azure_client_id,
                                            azure_client_secret,
                                            azure_tenant,
                                            params))

    logging.info('Step 1 has ended successfully')
    logging.info('Step 2: Getting users from data portail data base (dynamoDB) ')
    # Getting dp users
    users_dp =get_dp_data(aws_access_key_id,
                          aws_secret_access_key,
                          region_name,params["users_table"],attributes_list=["user_id","groups"])
    df_users_dp=pd.DataFrame(users_dp,columns=['user_id','groups'])
    print('NUmber of dp ysers',len(df_users_dp.index))
    logging.info('Step 3: Adding new users to dynamodb')

    # Selecting new users to insert
    df_new_users = pd.merge(df_users_ad,df_users_dp[["user_id"]],how='left', indicator=True, left_on='user_id',
                                      right_on='user_id').query("_merge == 'left_only'").drop(columns=['_merge'])
    # Inserting new users to db
    dict_new_users = df_new_users.T.to_dict().values()
    push_ad_data(aws_access_key_id,aws_secret_access_key,region_name,table_name=params['users_table'],data=dict_new_users)
    logging.info('Step 3 : has ended successfully')
    logging.info('Step 4 : Update user groups')

    #select existing users
    df_existing_users = pd.merge(df_users_ad,df_users_dp,left_on='user_id',right_on='user_id')
    #Getting dp native groups
    dp_groups = get_dp_data(aws_access_key_id,
                           aws_secret_access_key,
                           region_name,
                           params['datalake_groups_table'],
                           filters_list=["#tp = :t", "attribute_exists(policies)"],
                           attributes_list=['group_key'],
                           attribute_names={"#tp": "type"},
                           attribute_values={":t": "native"})
    list_dp_groups = [grp['group_key'] for grp in dp_groups]
    df_existing_users['user_native_groups'] = pd.Series([el for el in df_existing_users['groups_y'] if el in list_dp_groups])
    #Keep only users with AD groups
    df_existing_users['ad_groups'] = df_existing_users.apply(lambda row: row['groups_y'].difference(['user_native_groups']), axis=1)
    #Exclude users with no AD groups change
    df_existing_users= df_existing_users[df_existing_users.ad_groups!= df_existing_users.groups_x]
    #Getting the new groups list for users
    df_existing_users['updated_groups'] = np.where(df_existing_users.user_native_groups.notnull(),
                                                   df_existing_users['groups_x'] + df_existing_users['user_native_groups'],
                                                   df_existing_users['groups_x'])

    users_to_update = df_existing_users[['user_id', 'updated_groups']].T.to_dict().values()
    users_DBtable= get_dynamodb_table(aws_access_key_id,aws_secret_access_key,region_name,params['users_table'])
    for item in users_to_update:
        a = users_DBtable.update_item(
            Key={
                'user_id': item['user_id']
            }
            , UpdateExpression="set groups= :g"
            , ExpressionAttributeValues={
                ":g": set(item['updated_groups'])
            }
            , ReturnValues="UPDATED_NEW"

        )

    logging.info('End of process : Updating data portail users')








