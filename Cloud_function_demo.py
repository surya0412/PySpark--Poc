# Function dependencies, for example:
# package>=version
# requests-toolbelt==0.10.0
# google-auth==2.6.2

from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests


IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

# If you are using the stable API, set this value to False
USE_EXPERIMENTAL_API = True


def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.
    """
    print(data)
    print(type(data))

    client_id = '411093190695-mcrho4reumd4jonfcc3ssnn1rrgtre94.apps.googleusercontent.com'

    # {tenant-project-id}.appspot.com
    webserver_id = 't57133b650b9f4b40p-tp'

    # The name of the DAG you wish to trigger
    dag_name = 'My_sample_DAG'

    if USE_EXPERIMENTAL_API:
        endpoint = f'api/experimental/dags/{dag_name}/dag_runs'
        json_data = {'conf': data, 'replace_microseconds': 'false'}
    else:
        endpoint = f'api/v1/dags/{dag_name}/dagRuns'
        json_data = {'conf': data}
    webserver_url = (
        'https://'
        + webserver_id
        + '.appspot.com/'
        + endpoint
    )

    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        webserver_url, client_id, method='POST', json=json_data)


def make_iap_request(url, client_id, method='GET', **kwargs):

    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text