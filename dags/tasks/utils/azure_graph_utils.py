
def get_graphrbac_client(client_id, client_secret,tenant):

    from azure.common.credentials import ServicePrincipalCredentials
    from azure.graphrbac import GraphRbacManagementClient

    credentials = ServicePrincipalCredentials(
        client_id = client_id,
        secret = client_secret,
        tenant = tenant,
        resource="https://graph.windows.net"
    )

    graphrbac_client = GraphRbacManagementClient(
        credentials,
        tenant
    )

    return graphrbac_client


#Generate a valid token
def generate_token(azure_client_secret,azure_client_id):

    import adal
    resource ="https://graph.windows.net/"
    authority_url = 'https://login.microsoftonline.com/pernodricard.onmicrosoft.com'
    auth_context = adal.AuthenticationContext(authority_url)
    token = auth_context.acquire_token_with_client_credentials(resource, azure_client_id, azure_client_secret)[
        'accessToken']
    return token


#Get all items from a page returned in the response
def get_api_page_items(odata_next_page, query,params,header,azure_tenant):

    import json
    import requests

    resource = "https://graph.windows.net/"
    api_url = resource + azure_tenant + "/"
    if  odata_next_page is None:
        api_url = api_url +query+"/"
        parameters = params
    else:
        api_url = api_url + "/" + odata_next_page
        parameters = {
            'api-version':params['api-version']
        }
    response = json.loads(requests.get(api_url, params=parameters,headers=header).text)
    return response

#Get all items from all pages
def get_api_all_pages_items(query,params,header,azure_tenant):

    #Loop parameters initialization
    more_pages=True
    next_page=None
    items =[]
    while more_pages:
        response = get_api_page_items(next_page,query, params, header, azure_tenant)
        items.extend(response['value'])

        next_page = response.get('odata.nextLink', '')
        more_pages = False if next_page == '' else True
    return items


#Get all object (User or Group by specifing the type parameter) member within a given group
def get_group_object_members(id,header,azure_tenant,parameters,type=None):

    query = 'groups/' + id + '/members'
    response = get_api_all_pages_items(query,parameters,header,azure_tenant)
    if type is None:
        items = [item for item in response]
    elif type.lower() in ['user','group']:
        items = [item for item in response if item['objectType'].lower() == type]
    else:
        raise Exception('type parameter is invalid')
    return items

#Get all users from a given group and its subgroups
def get_users_recursively(id_group,header,azure_tenant,parameters):
    response = get_group_object_members(id_group,header,azure_tenant,parameters)
    object_list = response
    while any(el['objectType'].lower() == 'group' for el in object_list):
        groups_gen = [grp for grp in object_list if grp['objectType'].lower() == 'group']
        for grp in groups_gen:

            object_list.extend(get_group_object_members(grp['objectId'],header,azure_tenant,parameters))
            object_list.remove(grp)
    user_list=set([user['objectId'] for user in object_list])
    return list(user_list)


