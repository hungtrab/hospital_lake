"""Small Trino HTTP query client for in-cluster probes (no CLI dependency)."""
from __future__ import annotations
import json
import time
import urllib.parse
import urllib.request


def query(url, sql, timeout=120):
    base=url.rstrip('/')
    deadline=time.monotonic()+timeout
    request=urllib.request.Request(base+'/v1/statement', data=sql.encode(),
                                   headers={'X-Trino-User':'hospital-probe', 'Content-Type':'text/plain; charset=utf-8'})
    rows=[]
    while True:
        remaining=deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Trino query did not complete before deadline')
        with urllib.request.urlopen(request,timeout=min(30,remaining)) as response:
            result=json.load(response)
        if result.get('error'):
            raise RuntimeError('Trino: '+result['error'].get('message','query failed'))
        rows.extend(result.get('data',[]))
        if not result.get('nextUri'):
            return rows
        next_uri=result['nextUri']
        if urllib.parse.urlsplit(next_uri).netloc != urllib.parse.urlsplit(base).netloc:
            raise ValueError('unexpected Trino continuation host')
        request=urllib.request.Request(next_uri,headers={'X-Trino-User':'hospital-probe'})
