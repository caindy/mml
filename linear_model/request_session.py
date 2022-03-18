from urllib3.util.retry import Retry
from requests import Session
from requests.sessions import HTTPAdapter

retries = Retry(total=5, backoff_factor=0.1,
                status_forcelist=[500, 502, 503, 504])

def get_session(protocol = 'http://'):
    s = Session()
    s.mount(protocol, HTTPAdapter(max_retries=retries))
    return s