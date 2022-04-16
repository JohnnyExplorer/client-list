from asyncio.windows_events import NULL
from script.config import API_HOST, API_PORT, API_TOKEN_URL, RESOURCE, CLIENTID, CLIENT_SECRET_KEY, GRANTTYPE
import requests
import json





def getToken():
    url = API_TOKEN_URL
    try:
        data = {
            'resource': RESOURCE,
            'client_id': CLIENTID,
            'client_secret': CLIENTSECRETKEY,
            'grant_type': GRANTTYPE
        }
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        print('get token data {}'.format(data))
        return requests.post(url, data=json.dumps(data), headers=headers)
    except:
        print('failed getting Token')

def get(path): 
    url = 'http://{}/{}'.format(API_HOST,path)
    try:
        data = NULL
        token = getToken().access_token
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'Authorization': token}
        print('Calling API {}'.format(data))
        return requests.get(url, data=json.dumps(data), headers=headers)
    except:
        print('failed contacting API')

