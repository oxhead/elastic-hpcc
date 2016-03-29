import json
import random
import requests

from gevent import monkey

monkey.patch_all()


def pretty_print_POST(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in
    this function because it is programmed to be pretty
    printed and may differ from the actual request.
    """
    print('{}\n{}\n{}\n\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))


def query_originalperson():
    headers = {
        "Content-Type": "application/json"
    }

    url = "http://152.46.16.135:8002/WsEcl/json/query/roxie/fetchpeoplebyzipservice"
    payload = {
        "fetchpeoplebyzipservice": {
            "zipvalue": "27616"
        }
    }

    req = requests.Request('POST', url, headers=headers, data=json.dumps(payload))
    prepared = req.prepare()

    #pretty_print_POST(prepared)

    session = requests.Session()
    r = session.send(prepared)
    print(r.text)


def query_anagram2():
    headers = {
        "Content-Type": "application/json"
    }

    url = "http://152.46.16.135:8002/WsEcl/json/query/roxie/validateanagrams"
    payload = {
        "validateanagrams": {
            "word": "test"
        }
    }

    req = requests.Request('POST', url, headers=headers, data=json.dumps(payload))
    prepared = req.prepare()
    session = requests.Session()
    r = session.send(prepared)
    print(r.text)


def query_sixdegree():
    headers = {
        "Content-Type": "application/json"
    }

    url = "http://152.46.16.135:8002/WsEcl/json/query/roxie/searchlinks"
    payload = {
        "searchlinks": {
            "name": "Everingham, Andi"
        }
    }

    req = requests.Request('POST', url, headers=headers, data=json.dumps(payload))
    prepared = req.prepare()
    session = requests.Session()
    r = session.send(prepared)
    print(r.text)


class ConnectionFactory:

    @staticmethod
    def new():
        session = requests.Session()
        session.headers = {
            "Content-Type": "application/json"
        }
        return session


class QueryFactory:
    def __init__(self, applications):
        self.applications = applications
        self.router_table = {
            "anagram2": query_anagram2,
            "originalperson": query_originalperson,
            "sixdegree": query_sixdegree,
        }

    def next(self):
        application = random.choice(self.applications)
        return self.router_table[application.lower()]
