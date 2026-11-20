from http.server import ThreadingHTTPServer
import importlib.util
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from urllib.error import HTTPError
from urllib.request import Request,urlopen

ROOT=Path(__file__).resolve().parents[2]
SPEC=importlib.util.spec_from_file_location('hospital_api',ROOT/'dashboard/hospital/server.py')
MODULE=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MODULE)


class HospitalAPITests(unittest.TestCase):
    def setUp(self):
        self.service='s'*64;self.admin='a'*64
        self.enabled=False
        def toggle(value):
            if type(value) is not bool:raise ValueError('enabled must be boolean')
            self.enabled=value;return dict(enabled=value)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),MODULE.Handler)
        self.server.app=SimpleNamespace(service_token=self.service,admin_token=self.admin,producer_error=None,
            daily=SimpleNamespace(set_enabled=toggle,status=lambda:dict(enabled=self.enabled)))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join()

    def request(self,path,body=None,headers=None):
        req=Request(self.url+path,data=json.dumps(body).encode() if body is not None else None,headers=headers or {})
        try:
            with urlopen(req,timeout=5) as response:return response.status,json.load(response)
        except HTTPError as exc:return exc.code,json.load(exc)

    def test_read_and_write_authentication(self):
        self.assertEqual(self.request('/healthz')[0],200)
        self.assertEqual(self.request('/api/status')[0],401)
        headers={'X-Backend-Token':self.service}
        self.assertEqual(self.request('/api/status',headers=headers)[0],200)
        self.assertEqual(self.request('/api/simulation',dict(enabled=True),headers)[0],403)
        headers['X-Admin-Token']=self.admin
        self.assertEqual(self.request('/api/simulation',dict(enabled=True),headers),(200,dict(enabled=True)))
        self.assertEqual(self.request('/api/simulation',dict(enabled='true'),headers)[0],400)

    def test_no_arbitrary_sql_or_commands(self):
        self.assertEqual(self.request('/api/query',headers={'X-Backend-Token':self.service})[0],404)
        with self.assertRaises(ValueError): MODULE.Application.timeline(None,"x' OR 1=1 --")


if __name__=='__main__':unittest.main()
