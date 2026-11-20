import io
import json
from unittest.mock import patch
import pytest
from spark.common.trino import query


def test_pages_keep_types_and_propagate_errors():
    pages=[io.BytesIO(json.dumps(dict(data=[[1]],nextUri='http://trino:8080/v1/next')).encode()),
           io.BytesIO(json.dumps(dict(data=[[2,None]])).encode())]
    with patch('urllib.request.urlopen',side_effect=pages):
        assert query('http://trino:8080','SELECT n FROM fixture') == [[1],[2,None]]
    with patch('urllib.request.urlopen',return_value=io.BytesIO(b'{"error":{"message":"missing table"}}')):
        with pytest.raises(RuntimeError,match='missing table'):
            query('http://trino:8080','SELECT n FROM missing')


def test_unexpected_continuation_host_rejected():
    with patch('urllib.request.urlopen',return_value=io.BytesIO(b'{"nextUri":"http://unexpected:80/"}')):
        with pytest.raises(ValueError,match='continuation host'):
            query('http://trino:8080','SELECT n FROM fixture')
