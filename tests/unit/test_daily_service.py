from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from generators.stream_generator.daily_service import DailyService, journey
from spark.streaming.realtime_aggregates import payload_values


class DailyServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'daily.sqlite'
        self.service=DailyService(self.path,start_date='2026-11-21',visits=2)
        self.now=datetime(2026,11,21,23,tzinfo=timezone.utc)
        self.sent=[]

    def send(self,key,body):
        self.sent.append(json.loads(body));return SimpleNamespace(partition=0,offset=len(self.sent)-1)

    def test_paused_and_not_before_start(self):
        self.assertEqual(self.service.tick(self.send,self.now),0)
        self.service.set_enabled(True)
        self.assertEqual(self.service.tick(self.send,datetime(2026,11,19,tzinfo=timezone.utc)),0)
        self.assertEqual(self.service.status()['planned_events'],0)

    def test_resume_and_retry_use_stable_ids(self):
        self.service.set_enabled(True)
        self.assertGreater(self.service.tick(self.send,self.now),0)
        before=len(self.sent)
        restarted=DailyService(self.path,start_date='2026-11-21',visits=2)
        self.assertEqual(restarted.tick(self.send,self.now),0)
        self.assertEqual(len(self.sent),before)
        restarted.set_enabled(False)
        self.assertFalse(self.service.status()['enabled'])
        with self.assertRaises(ValueError): DailyService(self.path,visits=3)

    def test_failed_ack_leaves_event_pending(self):
        self.service.set_enabled(True)
        attempted=[]
        def failure(key,body): attempted.append(json.loads(body));raise TimeoutError('broker unavailable')
        with self.assertRaises(TimeoutError): self.service.tick(failure,self.now)
        self.assertEqual(self.service.status()['sent_events'],0)
        self.service.tick(self.send,self.now)
        self.assertEqual(attempted[0],self.sent[0])

    def test_horizon_stops_arrivals_but_drains_overnight_discharges(self):
        self.service.set_enabled(True)
        self.service.tick(self.send,datetime(2026,12,25,tzinfo=timezone.utc),limit=1000)
        with self.service.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plans').fetchone()[0],31)
            self.assertEqual(db.execute('SELECT max(day) FROM plans').fetchone()[0],'2026-12-21')
        status=self.service.status()
        self.assertEqual(status['sent_events'],status['planned_events'])
        self.assertEqual(sum(r['event_type']=='ADMISSION' for r in self.sent),62)
        self.assertEqual(sum(r['event_type']=='DISCHARGE' for r in self.sent),62)

    def test_plans_are_immutable_and_lifecycle_payloads_valid(self):
        self.service.plan_day('2026-11-21')
        initial=self.service.status()['planned_events']
        self.service.plan_day('2026-11-21')
        self.assertEqual(self.service.status()['planned_events'],initial)
        overnight=False
        for i in range(40):
            rows=journey(dict(visit_id=f'VISIT-20261121-{i:06d}',arrival_time='2026-11-21T01:00:00Z'),42)
            self.assertEqual(rows,journey(dict(visit_id=f'VISIT-20261121-{i:06d}',arrival_time='2026-11-21T01:00:00Z'),42))
            self.assertEqual(rows[-1]['event_type'],'DISCHARGE')
            self.assertEqual(len(rows),len({r['event_id'] for r in rows}))
            self.assertEqual([r['event_time'] for r in rows],sorted(r['event_time'] for r in rows))
            for row in rows:
                self.assertIsNone(payload_values(json.dumps(row['payload']),row['event_type'],1)[-1],row)
            overnight |= rows[-1]['event_time'][:10]>'2026-11-21'
        self.assertTrue(overnight)


if __name__=='__main__':unittest.main()
