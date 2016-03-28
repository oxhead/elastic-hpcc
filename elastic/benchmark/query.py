import json
import requests


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


def run_query():
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
    #print(r.text)