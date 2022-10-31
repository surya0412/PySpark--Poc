"""Example use of a service account to authenticate to Identity-Aware Proxy."""

# [START iap_make_request]
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/002V42744/Downloads/model-hexagon-316809-c525f1dfc89e.json"


client_id = '751791657526-elq3bg2vsmd2mstg39tr0rla1q9vfgci.apps.googleusercontent.com'
    # This should be part of your webserver's URL:
    # {tenant-project-id}.appspot.com
webserver_id = 'k36bc1065c6f4f2f7p-tp'
# The name of the DAG you wish to trigger
dag_name = 'My_sample_DAG'

if True:
    endpoint = f'api/experimental/dags/{dag_name}/dag_runs'
    json_data = {'conf': {}, 'replace_microseconds': 'false'}
else:
    endpoint = f'api/v1/dags/{dag_name}/dagRuns'
    json_data = {'conf': data}
webserver_url = (
    'https://'
    + webserver_id
    + '.appspot.com/'
    + endpoint
)
print(webserver_url)
def make_iap_request(url, client_id, method='GET', **kwargs):

    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    open_id_connect_token = id_token.fetch_id_token(Request(), client_id)
    print(open_id_connect_token)
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text

# [END iap_make_request]

# Make a POST request to IAP which then Triggers the DAG
make_iap_request(
    webserver_url, client_id, method='POST', json=json_data)
