"""Deterministic payload and serving-client failure contracts."""
import json
import unittest
from unittest.mock import patch
from datetime import datetime, timezone

from spark.common.clickhouse import ClickHouse
from spark.streaming.realtime_aggregates import payload_values


class PayloadTests(unittest.TestCase):
    def test_known_alias_and_unknown_department(self):
        p = dict(patient_id="P1", encounter_id="E1", department=" ER ", encounter_type="emergency")
        self.assertEqual(payload_values(json.dumps(p), "ADMISSION", 1), ("P1", "E1", "EMERGENCY", False, None))
        p["department"] = "ER123"
        self.assertEqual(payload_values(json.dumps(p), "ADMISSION", 1)[-1], "UNKNOWN_DEPARTMENT")

    def test_invalid_envelope_payload_and_version(self):
        for payload in (None, "null", "{bad", "[]"):
            self.assertEqual(payload_values(payload, "ADMISSION", 1)[-1], "INVALID_PAYLOAD")
        self.assertEqual(payload_values('{}', "ADMISSION", 2)[-1], "UNSUPPORTED_VERSION")
        self.assertEqual(payload_values('{}', "ADMISSION", 1)[-1], "MISSING_PAYLOAD_ID")

    def test_lab_flag_computed_and_nonfinite_rejected(self):
        p = dict(patient_id="P1", encounter_id="E1", test_code="GLU", test_name="Glucose",
                 unit="mg/dL", result_value=200, reference_low=70, reference_high=99, is_abnormal=False)
        self.assertEqual(payload_values(json.dumps(p), "LAB_RESULT", 1)[3:], (True, None))
        for bad in (float('nan'), float('inf'), True, "200"):
            p['result_value'] = bad
            self.assertEqual(payload_values(json.dumps(p), "LAB_RESULT", 1)[-1], "INVALID_LAB_VALUE")


class ClickHouseClientTests(unittest.TestCase):
    def test_chunks_and_naive_utc_serialization(self):
        client = ClickHouse(password="synthetic")
        with patch.object(client, "execute") as execute:
            client.insert("lab_snapshots", (dict(snapshot_version=1, window_start=datetime(2026, 1, 1), abnormal_labs=1)
                for _ in range(1001)))
        self.assertEqual(execute.call_count, 2)
        self.assertEqual(len(execute.call_args_list[0].args[1].splitlines()), 1000)
        self.assertIn(b'2026-01-01 00:00:00.000000', execute.call_args.args[1])

    def test_retry_exhaustion_is_failure(self):
        client = ClickHouse(password="synthetic", retries=2)
        with patch('urllib.request.urlopen', side_effect=TimeoutError), patch('time.sleep'), self.assertRaises(TimeoutError):
            client.execute('SELECT 1')
        with self.assertRaises(ValueError):
            ClickHouse(password="synthetic", retries=0)


if __name__ == '__main__':
    unittest.main()
