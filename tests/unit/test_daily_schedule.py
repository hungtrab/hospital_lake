import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from generators.stream_generator.daily_schedule import build_schedule

ROOT = Path(__file__).resolve().parents[2]


def config():
    return dict(date="2026-11-23", timezone="Asia/Ho_Chi_Minh", seed=42, visits=2,
                slots=[dict(start="08:00", end="08:01", weight=1)])


class DailyScheduleTests(unittest.TestCase):
    def test_exact_fixture(self):
        self.assertEqual(build_schedule(config()), [
            dict(visit_id="VISIT-20261123-000001", arrival_time="2026-11-23T01:00:00Z"),
            dict(visit_id="VISIT-20261123-000002", arrival_time="2026-11-23T01:00:00Z")])

    def test_reproducible_sorted_and_inside_local_slots(self):
        value = config()
        value.update(visits=100, slots=[dict(start="08:00", end="12:00", weight=3),
                                        dict(start="14:00", end="18:00", weight=1)])
        original = copy.deepcopy(value)
        rows = build_schedule(value)
        self.assertEqual(rows, build_schedule(value))
        self.assertEqual(value, original)
        self.assertEqual(len(rows), 100)
        self.assertEqual(len({r['visit_id'] for r in rows}), 100)
        times = [r['arrival_time'] for r in rows]
        self.assertEqual(times, sorted(times))
        for raw in times:
            local = datetime.fromisoformat(raw).astimezone(timezone(timedelta(hours=7)))
            self.assertEqual(local.date().isoformat(), value['date'])
            self.assertTrue(8 <= local.hour < 12 or 14 <= local.hour < 18)
            self.assertEqual(local.second, 0)

    def test_zero_visits_and_day_namespace(self):
        today = build_schedule(config())
        tomorrow = build_schedule(dict(config(), date="2026-11-24"))
        self.assertFalse({r['visit_id'] for r in today} & {r['visit_id'] for r in tomorrow})
        self.assertEqual(build_schedule(dict(config(), visits=0)), [])

    def test_midnight_and_end_of_day(self):
        for start, end, expected in [('00:00', '00:01', '2026-11-22T17:00:00Z'),
                                      ('23:59', '24:00', '2026-11-23T16:59:00Z')]:
            value = dict(config(), slots=[dict(start=start, end=end, weight=1)])
            self.assertEqual(build_schedule(value)[0]['arrival_time'], expected)

    def test_invalid_inputs(self):
        changes = [dict(date='2026-02-30'), dict(date='20261123'), dict(timezone='UTC'),
                   dict(seed=True), dict(seed=1.5), dict(visits=True), dict(visits=-1),
                   dict(visits=2.5), dict(slots=[]), dict(slots=[None])]
        for start, end, weight in [('08:00','08:00',1), ('09:00','08:00',1),
                                   ('8:00','09:00',1), ('24:00','24:00',1),
                                   ('08:00','08:60',1), ('08:00','09:00',0),
                                   ('08:00','09:00',True), ('08:00','09:00',float('nan')),
                                   ('08:00','09:00',float('inf'))]:
            changes.append(dict(slots=[dict(start=start,end=end,weight=weight)]))
        changes.append(dict(slots=[dict(start='08:00',end='10:00',weight=1),
                                   dict(start='09:00',end='11:00',weight=1)]))
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                build_schedule(dict(config(), **change))
        for value in [None, [], {}, dict(config(), date='0001-01-01',
                                       slots=[dict(start='00:00',end='00:01',weight=1)])]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_schedule(value)

    def test_large_finite_weights(self):
        value = dict(config(), slots=[dict(start='08:00',end='08:01',weight=1e308),
                                      dict(start='09:00',end='09:01',weight=1e308)])
        self.assertEqual(len(build_schedule(value)), 2)

    def test_cli_success_and_failure_preserves_output(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            source, output = directory/'input.json', directory/'out.json'
            source.write_text(json.dumps(config()), encoding='utf-8')
            command = [sys.executable, '-S', str(ROOT/'generators/stream_generator/daily_schedule.py'),
                       '--config', str(source), '--output', str(output)]
            result = subprocess.run(command, capture_output=True, text=True, cwd=folder)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(output.read_text()), build_schedule(config()))
            previous = output.read_bytes()
            source.write_text('{bad json', encoding='utf-8')
            result = subprocess.run(command, capture_output=True, text=True, cwd=folder)
            self.assertEqual(result.returncode, 2)
            self.assertTrue(result.stderr)
            self.assertEqual(output.read_bytes(), previous)
            self.assertFalse(list(directory.glob('.schedule-*')))


if __name__ == '__main__':
    unittest.main()
