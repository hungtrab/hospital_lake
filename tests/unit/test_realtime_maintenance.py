from unittest.mock import Mock
from scripts.realtime_maintenance import cleanup


def test_idle_latest_snapshot_is_never_deleted():
    client=Mock()
    client.rows.return_value=[dict(v=100)]
    report=cleanup(client,24,now_ns=200_000_000_000_000)
    assert report['deleted_before_version']==100
    for call in client.execute.call_args_list:
        assert 'snapshot_version < 100 ' in call.args[0]
    assert client.execute.call_count==4


def test_empty_publication_does_not_delete_anything():
    client=Mock()
    client.rows.return_value=[dict(v=0)]
    cleanup(client,24)
    client.execute.assert_not_called()
