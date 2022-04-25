#from util.config import API_HOST, API_PORT, API_TOKEN_URL, RESOURCE, CLIENTID, CLIENT_SECRET_KEY, GRANTTYPE
import requests
import json


#oauth
API_TOKEN_URL = ''
RESOURCE = ''
CLIENTID = ''
CLIENT_SECRET_KEY = ''

#api
API_HOST = ''

file_path = '../data/donation/'
file_name = 'contacts.json'

def getToken():
    url = API_TOKEN_URL
    try:
        payload = {
            'resource': RESOURCE,
            'client_id': CLIENTID,
            'client_secret': CLIENT_SECRET_KEY,
            'grant_type': 'client_credentials'
        }
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        print('get token data {}'.format(payload))
        return requests.post(url, data=payload, headers=headers)
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")

def getDonation(path):
    url = '{}{}'.format(API_HOST,path)
    token = getToken().json()['access_token']
    #print(f"token {token}")
    try:
        headers = {'Authorization': token}
        return requests.get(url, headers=headers)
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")

def save(ts,data):
    with open(f'{file_path}{ts}{file_name}', mode='a', encoding='utf-8') as datalog:
        datalog.write(json.dumps(data.json()['value']))

ts = '1111'
res = getDonation('/api/data/v9.0/contacts')
save(ts,res)
