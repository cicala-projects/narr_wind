import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(
    retries=10,
    timeout=5,
    backoff_factor=1, 
    status_forcelist=(500, 502, 504, 503),
    session=None):

    """
    Retry a failed request using a back-off function and a scheduled retry.
    This function will take a request.Session object and give it a new set of
    parameters to retry a request automatically. The reason behind this is
    because usually failed requests *might* work with a retry (load-balancing 
    reasons in the source server). 
    (Taken from: https://www.peterbe.com/plog/best-practice-with-retries-with-requests)
    """

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

