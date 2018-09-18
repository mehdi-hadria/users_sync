

def syn_dp_ad_groups(
        aws_access_key_id,
        aws_secret_access_key,
        region_name,
        azure_client_id,
        azure_client_secret,
        azure_tenant,
        params,
        script_path
                    ):

    import sys, os
    import datetime
    import pandas as pd
    import logging

    sys.path.insert(0, script_path)
    from tasks.utils.azure_graph_utils import get_graphrbac_client
    from tasks.utils.dynamodb_utils import get_dp_data, delete_dp_data, push_ad_data


    logging.info('Starting process: Loading data to active-directory-groups table')
    logging.info('Step 1: Getting groups from Azure AD')

    graphrbac_client = get_graphrbac_client(azure_client_id,azure_client_secret,azure_tenant)
    group_operations = graphrbac_client.groups
    groups_ad_list = list(group_operations.list())
    extracted_ad_groups = []
    for grp in groups_ad_list:
        grp = grp.as_dict()
        new_grp = {
            'ad_id': grp.get('object_id', None),
            'name': grp.get('display_name', None),
            'owner': 'romain.nio@pernod-ricard.com',
            'created_at': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            'name_to_search': grp.get('display_name', '').lower()
        }
        extracted_ad_groups.append(new_grp)

    df_groups_ad = pd.DataFrame(extracted_ad_groups)

    logging.info('Step 1 has ended successfully')
    logging.info('Step 2: Getting groups from data portail data base (dynamoDB) ')

    groups_dp = get_dp_data(aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name, table_name=params['groups_table'],
                        attributes_list=['ad_id']
                            )
    df_groups_dp_ad= pd.DataFrame(groups_dp,columns=['ad_id'])

    logging.info('Step 2 has ended successfully')
    logging.info('Step 3 Deleting AD groups which have been removed from Azure AD')
    delta_df_ad_ad = df_groups_dp_ad.merge(df_groups_ad, how='left', indicator=True, left_on='ad_id', right_on='ad_id')
    groups_to_delete = delta_df_ad_ad.ad_id[delta_df_ad_ad['_merge'] == 'left_only'].tolist()

    # Deleting groups:
    logging.info('Step 3 Number of groups to delete : ', len(groups_to_delete))
    delete_dp_data(aws_access_key_id, aws_secret_access_key, region_name, table_name=params['groups_table'], keys_list=groups_to_delete)
    logging.info('Step 3 has ended successfully')
    logging.info('Step 4: Inserting new groups to dynamodb')

    # Inserting new groups :
    new_groups = pd.merge(df_groups_ad, df_groups_dp_ad[['ad_id']], how='left', indicator=True, left_on='ad_id',
                          right_on='ad_id').query("_merge == 'left_only'").drop(columns=['_merge'])
    logging.info('Step 4: Number of new group to add',len(new_groups.index))
    dict_new_groups = new_groups.T.to_dict().values()
    push_ad_data(aws_access_key_id,aws_secret_access_key,region_name,table_name=params['groups_table'],data=dict_new_groups)

    logging.info('Step 4 has ended successfully')
    logging.info('End of process : Updating data portail groups')




