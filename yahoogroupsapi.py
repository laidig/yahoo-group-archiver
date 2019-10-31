from __future__ import unicode_literals
from contextlib import contextmanager
import functools
import logging
import os
import random
import time

try:
    from warcio.capture_http import capture_http
    warcio_failed = False
except ImportError as e:
    warcio_failed = e

import requests  # Must be imported after capture_http
from requests.exceptions import Timeout, ConnectionError, HTTPError

VERIFY_HTTPS = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'yahoogroups_cert_chain.pem')


@contextmanager
def dummy_contextmanager(*kargs, **kwargs):
    yield


class YGAException(Exception):
    pass

class Unrecoverable(YGAException):
    pass

class AuthenticationError(Unrecoverable):
    pass

class NotFound(Unrecoverable):
    pass

class Recoverable(YGAException):
    pass


def backoff_time(attempt):
    return random.randint(2**(attempt), 2**(attempt+1))


class YahooGroupsAPI:
    BASE_URI = "https://groups.yahoo.com/api"

    API_VERSIONS = {
            'HackGroupInfo': 'v1',  # In reality, this will get the root endpoint
            'messages': 'v1',
            'files': 'v2',
            'albums': 'v2',         # v3 is available, but changes where photos are located in json
            'database': 'v1',
            'links': 'v1',
            'statistics': 'v1',
            'polls': 'v1',
            'attachments': 'v1',
            'members': 'v1'
            }

    logger = logging.getLogger(name="YahooGroupsAPI")

    s = None
    ww = None
    http_context = dummy_contextmanager

    def __init__(self, group, cookie_jar=None, headers={}, delay=0):
        self.s = requests.Session()
        self.group = group
        self.delay = delay

        if cookie_jar:
            self.s.cookies = cookie_jar
        self.s.headers = {'Referer': self.BASE_URI}
        self.s.headers.update(headers)

    def set_warc_writer(self, ww):
        if ww is not None and warcio_failed:
            self.logger.fatal("Attempting to log to warc, but warcio failed to import.")
            raise warcio_failed
        self.ww = ww
        self.http_context = capture_http

    def __getattr__(self, name):
        """
        Easy, human-readable REST stub, eg:
           yga.messages(123, 'raw')
           yga.messages(count=50)
        """
        if name not in self.API_VERSIONS:
            raise AttributeError()
        return functools.partial(self.get_json, name)

    def download_file(self, url, f=None, **args):
        with self.http_context(self.ww):
            retries = 5
            while True:
                time.sleep(self.delay)
                r = self.s.get(url, stream=True, verify=VERIFY_HTTPS, **args)
                if r.status_code == 400 and retries > 0:
                    self.logger.info("Got 400 error for %s, will sleep and retry %d times", url, retries)
                    retries -= 1
                    time.sleep(5)
                    continue
                r.raise_for_status()
                break

            if f is None:
                return r.content

            for chunk in r.iter_content(chunk_size=4096):
                f.write(chunk)

    def get_json(self, target, *parts, **opts):
        """Get an arbitrary endpoint and parse as json"""
        with self.http_context(self.ww):
            uri_parts = [self.BASE_URI, self.API_VERSIONS[target], 'groups', self.group, target]
            uri_parts = uri_parts + list(map(str, parts))

            if target == 'HackGroupInfo':
                uri_parts[4] = ''

            uri = "/".join(uri_parts)
            time.sleep(self.delay)

            tries = 5  # FIXME, customisable
            for attempt in range(tries):
                try:
                    try:
                        r = self.s.get(uri, params=opts, verify=VERIFY_HTTPS, allow_redirects=False, timeout=15)
                        r.raise_for_status()
                        # raise_for_status won't raise on 307s
                        if r.status_code != 200:
                            raise requests.exceptions.HTTPError(response=r)
                        return r.json()['ygData']
                    except (ConnectionError, Timeout) as e:
                        self.logger.warning("Network error, attempt %d/%d, %s", attempt, tries, uri, e)
                        self.logger.debug("Exception detail:", exc_info=e)
                        raise Recoverable
                    except HTTPError as e:
                        ygError = {}
                        try:
                            ygError = e.response.json()['ygError']
                        except (ValueError, KeyError):
                            pass

                        code = e.response.status_code
                        if code == 307 or code == 401 or code == 403:
                            raise AuthenticationError
                        elif code == 404:
                            raise NotFound
                        else:
                            # TODO: Test ygError response?
                            raise Recoverable
                except Recoverable:
                    if attempt < tries - 1:
                        delay = backoff_time(i)
                        # TODO: some info logging here
                        time.sleep(delay)
                        continue
                    else:
                        raise
