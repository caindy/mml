import weakref
from requests import Session
from requests.sessions import HTTPAdapter
from urllib3.util.retry import Retry
from functools import cached_property, cache

@cache
def get_cached(url:str, s):
    print(f'Fetching {url}')
    return s().get(url)

class WebClient():
    __retries = Retry(total=5, backoff_factor=0.1,
                    status_forcelist=[500, 502, 503, 504])

    @cached_property
    def session(self):
        s = Session()
        s.mount('https://', HTTPAdapter(max_retries=WebClient.__retries))
        return s

    def get_cached(self, url):
        return get_cached(url, weakref.ref(self.session))
    
    def get(self, url):
        return self.session.get(url)