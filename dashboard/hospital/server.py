"""Hospital demo API and persistent daily producer; analytics come from Trino/ClickHouse."""
from __future__ import annotations

from datetime import date, datetime, timezone
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import re
import secrets
import threading
import time
from urllib.parse import parse_qs, urlsplit

from generators.stream_generator import manual_visits
from generators.stream_generator.daily_service import DailyService, LOCAL
from dashboard.hospital.room_map import WARD_LABELS, snapshot
from spark.common.clickhouse import ClickHouse
from spark.common.trino import query

ROOT = Path(__file__).resolve().parents[2]
LOG = logging.getLogger(__name__)
DAY_SQL = "CAST(at_timezone(event_time, 'Asia/Ho_Chi_Minh') AS DATE)"


class Application:
    def __init__(self):
        self.service_token = os.environ['HOSPITAL_SERVICE_TOKEN']
        self.admin_token = os.environ['HOSPITAL_ADMIN_TOKEN']
        if min(len(self.service_token),len(self.admin_token)) < 32:
            raise ValueError('API credentials must have at least 32 characters')
        state = Path(os.environ.get('HOSPITAL_STATE_DIR','/state'))
        state.mkdir(parents=True,exist_ok=True)
        self.lockfile = (state/'producer.lock').open('a')
        fcntl.flock(self.lockfile,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.daily = DailyService(state/'daily.sqlite',start_date=os.environ.get('SIM_START_DATE','2026-11-21'),
                                  visits=int(os.environ.get('SIM_DAILY_VISITS','5315')), profile=os.environ.get('SIM_PROFILE','bach-mai-v1'))
        self.trino = os.environ.get('TRINO_URL','http://trino:8080')
        self.clickhouse = ClickHouse(retries=1)
        self.cache, self.cache_lock = {}, threading.Lock()
        self.stop = threading.Event()
        self.producer_error = None
        self.thread = threading.Thread(target=self.produce,daemon=True)
        self.thread.start()

    def produce(self):
        producer = None
        try:
            while not self.stop.is_set():
                try:
                    if self.daily.enabled() or self.daily.pending_manual(limit=1):
                        if producer is None:
                            from kafka import KafkaProducer
                            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                                                     acks='all', retries=5, max_block_ms=15000)
                        def send(key, body):
                            return producer.send(os.environ.get('KAFKA_TOPICS','hospital.events'),key=key.encode(),value=body).get(timeout=30)
                        self.daily.tick(send)
                    self.producer_error = None
                except Exception:
                    LOG.exception('daily producer tick failed; pending events retained')
                    self.producer_error = 'Gửi sự kiện chưa thành công; dữ liệu chờ vẫn được giữ để thử lại.'
                self.stop.wait(1)
        finally:
            if producer is not None:
                producer.close(timeout=10)

    def sql(self, sql):
        return query(self.trino,sql,timeout=25)

    def cached(self, key, loader, ttl=15):
        with self.cache_lock:
            now=time.monotonic()
            if key in self.cache and now-self.cache[key][0]<ttl:
                return self.cache[key][1]
            result=loader()
            self.cache[key]=(time.monotonic(),result)
            return result

    def overview(self):
        def load():
            states=self.clickhouse.rows('SELECT * FROM hospital.realtime_hospital_state')
            today=datetime.now(LOCAL).date().isoformat()
            row=self.sql(f"SELECT count_if(event_type='ADMISSION'), count_if(event_type='DISCHARGE'), count_if(event_type='LAB_RESULT'), count_if(event_type='LAB_RESULT' AND is_abnormal), count_if(event_type='PATIENT_REGISTERED' AND encounter_id IS NOT NULL),count_if(event_type='OUTPATIENT_COMPLETED') FROM iceberg.silver.realtime_events WHERE {DAY_SQL}=DATE '{today}'")[0]
            active=self.sql("SELECT department,count(*) FROM iceberg.silver.realtime_encounters WHERE seen_admission AND event_type<>'DISCHARGE' GROUP BY department ORDER BY department")
            return dict(state=states[0] if states else None, date=today,
                        today=dict(admissions=row[0],discharges=row[1],labs=row[2],abnormal_labs=row[3],arrivals=row[4],outpatients=row[5]),
                        departments=[dict(department=r[0],patients=r[1]) for r in active],
                        queried_at=datetime.now(timezone.utc).isoformat())
        return self.cached('overview',load)

    def visit_charts(self):
        now = datetime.now(LOCAL)
        def load():
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            def utc_stamp(value):
                return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
            template = (ROOT/'sql/trino/live_visit_charts.sql').read_text()
            rows = self.sql(template.format(start_utc=utc_stamp(midnight), end_utc=utc_stamp(now)))
            hours = [dict(arrivals=0, admissions=0) for _ in range(now.hour + 1)]
            ages, conditions, waits = {}, {}, {}
            for kind, hour, age, condition, wait, count in rows:
                if kind == 'PATIENT_REGISTERED':
                    hours[hour]['arrivals'] += count
                    ages[age] = ages.get(age, 0) + count
                    conditions[condition] = conditions.get(condition, 0) + count
                elif kind == 'ADMISSION':
                    hours[hour]['admissions'] += count
                elif kind == 'CONSULTATION_STARTED':
                    waits[wait] = waits.get(wait, 0) + count
            return dict(as_of=now.isoformat(), hours=hours, ages=ages, conditions=conditions, waits=waits)
        return self.cached(('visit-charts',now.date()),load,ttl=90)

    def patients(self, filters=None):
        filters = filters or {}
        conditions = []
        term = filters.get('q', '').strip().lower()
        if len(term) > 120:
            raise ValueError('Mã tìm kiếm dài tối đa 120 ký tự.')
        if term:
            if term.startswith('bn-'):
                term = 'p-visit-' + term[3:]
            literal = term.replace("'", "''")
            conditions.append(f"(strpos(lower(patient_id), '{literal}')>0 OR strpos(lower(encounter_id), '{literal}')>0)")
        status = filters.get('status')
        if status:
            if status not in ('discharged','inpatient','completed','consulting','waiting'):
                raise ValueError('Trạng thái không hợp lệ.')
            conditions.append(f"status='{status}'")
        department = filters.get('department')
        if department == 'OUTPATIENT':
            conditions.append('department IS NULL')
        elif department:
            if department not in WARD_LABELS:
                raise ValueError('Khoa không hợp lệ.')
            conditions.append(f"department='{department}'")
        lab = filters.get('lab')
        if lab:
            lab_conditions = {'done':'labs>0', 'none':'labs=0', 'abnormal':'abnormal_labs>0',
                              'normal':'labs>0 AND abnormal_labs=0'}
            if lab not in lab_conditions:
                raise ValueError('Bộ lọc xét nghiệm không hợp lệ.')
            conditions.append(lab_conditions[lab])
        for key, operator in [('from','>='),('to','<=')]:
            if filters.get(key):
                try:
                    day = date.fromisoformat(filters[key]).isoformat()
                except ValueError:
                    raise ValueError('Ngày không hợp lệ.') from None
                if day != filters[key]:
                    raise ValueError('Ngày phải có dạng YYYY-MM-DD.')
                conditions.append(f"CAST(at_timezone(updated,'Asia/Ho_Chi_Minh') AS date) {operator} DATE '{day}'")
        if filters.get('from','') > filters.get('to','9999-12-31'):
            raise ValueError('Ngày kết thúc phải từ ngày bắt đầu trở đi.')
        paged = 'page' in filters
        page = filters.get('page','1')
        if not re.fullmatch(r'[1-9][0-9]{0,5}', page):
            raise ValueError('Trang không hợp lệ.')
        # All pages in one browse operation read the same Iceberg snapshot.
        as_of = filters.get('as_of') or datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        try:
            timestamp = datetime.fromisoformat(as_of)
            if timestamp.tzinfo is None:
                raise ValueError
            timestamp = timestamp.astimezone(timezone.utc)
        except (ValueError, OverflowError):
            raise ValueError('Mốc tra cứu không hợp lệ.') from None
        as_of = timestamp.isoformat(timespec='milliseconds')
        as_sql = as_of[:23].replace('T',' ')
        version = f" FOR TIMESTAMP AS OF TIMESTAMP '{as_sql} UTC'" if paged else ''
        size = 25 if paged else 100
        where = ' AND '.join(conditions) or 'TRUE'

        def load():
            rows = self.sql(f"""WITH visits AS (
                SELECT max_by(patient_id,ROW(event_time,event_id)) patient_id, encounter_id,
                    max_by(department,ROW(event_time,event_id)) FILTER (WHERE department IS NOT NULL) department,
                    max_by(event_type,ROW(event_time,event_id)) last_event, max(event_time) updated,
                    count_if(event_type='LAB_RESULT') labs,
                    count_if(event_type='LAB_RESULT' AND is_abnormal) abnormal_labs,
                    CASE WHEN count_if(event_type='DISCHARGE')>0 THEN 'discharged'
                        WHEN count_if(event_type='ADMISSION')>0 THEN 'inpatient'
                        WHEN count_if(event_type='OUTPATIENT_COMPLETED')>0 THEN 'completed'
                        WHEN count_if(event_type='CONSULTATION_STARTED')>0 THEN 'consulting' ELSE 'waiting' END status
                FROM iceberg.silver.realtime_events{version} WHERE encounter_id IS NOT NULL GROUP BY encounter_id)
                SELECT patient_id,encounter_id,department,last_event,to_iso8601(updated),status
                FROM visits WHERE {where} ORDER BY updated DESC,encounter_id
                OFFSET {(int(page)-1)*size} ROWS LIMIT {size+int(paged)}""")
            keys = ['patient_id','encounter_id','department','last_event','event_time','status']
            items = [dict(zip(keys,row)) for row in rows[:size]]
            return dict(items=items, more=len(rows)>size, as_of=as_of) if paged else items

        if not conditions and page == '1' and 'as_of' not in filters:
            return self.cached('patients-first' if paged else 'patients',load)
        return load()

    def timeline(self, encounter):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,120}',encounter):
            raise ValueError('Mã lượt khám không hợp lệ.')
        rows=self.sql(f"""WITH events AS (SELECT * FROM iceberg.silver.realtime_events WHERE encounter_id='{encounter}'),
            payload AS (SELECT _event_id, json_extract(try(json_parse(from_utf8(raw_payload))), '$.payload') p
                FROM iceberg.bronze.stream_events_raw WHERE _event_id IN (SELECT event_id FROM events))
            SELECT e.event_id,e.event_type,to_iso8601(e.event_time),e.department,e.is_abnormal,
                arbitrary(json_extract_scalar(p,'$.specialty')),arbitrary(json_extract_scalar(p,'$.room_id')),
                arbitrary(json_extract_scalar(p,'$.doctor_id')),arbitrary(json_extract_scalar(p,'$.nurse_id')),
                arbitrary(json_extract_scalar(p,'$.wait_seconds')),arbitrary(json_extract_scalar(p,'$.age_years')),
                arbitrary(json_extract_scalar(p,'$.condition_group')), arbitrary(json_extract_scalar(p,'$.diagnosis_code')),
                arbitrary(json_extract_scalar(p,'$.diagnosis_label'))
            FROM events e LEFT JOIN payload ON e.event_id=_event_id GROUP BY 1,2,3,4,5 ORDER BY 3,1 LIMIT 100""")
        keys=['event_id','event_type','event_time','department','is_abnormal','specialty','room','doctor','nurse','wait_seconds','age_years','condition_group','diagnosis_code','diagnosis_label']
        return [dict(zip(keys,r)) for r in rows]

    def rooms(self):
        def load():
            sql = (ROOT/'sql/trino/live_room_occupancy.sql').read_text()
            return snapshot(self.daily.profile, self.sql(sql))
        return self.cached('rooms', load)

    def history(self):
        def load():
            rows=self.sql(f"SELECT CAST({DAY_SQL} AS VARCHAR),count_if(event_type='ADMISSION'),count_if(event_type='DISCHARGE') FROM iceberg.silver.realtime_events GROUP BY 1 ORDER BY 1 DESC LIMIT 14")
            return [dict(date=r[0],admissions=r[1],discharges=r[2]) for r in rows]
        return self.cached('history',load)

    def weekly(self):
        def load():
            today=datetime.now(LOCAL).date().isoformat()
            sql=(ROOT/'sql/trino/department_week_comparison.sql').read_text().strip().rstrip(';')
            sql=sql[sql.index('WITH totals'):].replace('WITH totals AS (',f"""WITH department_daily_metrics AS (
              SELECT CAST(date AS VARCHAR) AS date,department,admissions FROM iceberg.gold.live_department_daily_metrics),
              report_period AS (SELECT CAST(DATE '{today}' - INTERVAL '14' DAY AS VARCHAR) AS previous_start,
              CAST(DATE '{today}' - INTERVAL '7' DAY AS VARCHAR) AS current_start, '{today}' AS current_end), totals AS (""",1)
            rows=self.sql(sql)
            return [dict(zip(['department','previous_admissions','current_admissions','delta','change_pct'],r)) for r in rows]
        return self.cached('weekly',load)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(35)

    def log_message(self, fmt, *args):
        LOG.info('%s %s',self.command,urlsplit(self.path).path)

    def reply(self, status, payload):
        body=json.dumps(payload,ensure_ascii=False,allow_nan=False).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.end_headers()
        self.wfile.write(body)

    def authorized(self, admin=False):
        app=self.server.app
        if not secrets.compare_digest(self.headers.get('X-Backend-Token','').encode(),app.service_token.encode()):
            self.reply(401,dict(error='Unauthorized'))
            return False
        if admin and not secrets.compare_digest(self.headers.get('X-Admin-Token','').encode(),app.admin_token.encode()):
            self.reply(403,dict(error='Khóa quản lý không đúng.'))
            return False
        return True

    def do_GET(self):
        path=urlsplit(self.path)
        if path.path=='/healthz':
            return self.reply(200,dict(status='ok'))
        if not self.authorized(): return
        app=self.server.app
        try:
            if path.path=='/api/status': result=dict(simulation=app.daily.status(),producer_error=app.producer_error,synthetic=True)
            elif path.path=='/api/overview':
                result=app.overview()
                if parse_qs(path.query).get('charts') == ['1']:
                    try:
                        charts=app.visit_charts()
                    except Exception:
                        LOG.exception('visit chart query failed')
                        charts=None
                    result=dict(result,charts=charts)
            elif path.path=='/api/manual': result=manual_visits.listing(app.daily)
            elif path.path=='/api/rooms': result=app.rooms()
            elif path.path=='/api/patients':
                params = parse_qs(path.query, max_num_fields=12)
                result=app.patients({k:v[0] for k,v in params.items()})
            elif path.path=='/api/history': result=app.history()
            elif path.path=='/api/weekly': result=app.weekly()
            elif path.path=='/api/timeline': result=app.timeline(parse_qs(path.query).get('encounter_id',[''])[0])
            else: return self.reply(404,dict(error='Not found'))
            self.reply(200,result)
        except ValueError as exc:
            self.reply(400,dict(error=str(exc)))
        except Exception:
            LOG.exception('analytics request failed')
            self.reply(503,dict(error='Nguồn dữ liệu đang chưa sẵn sàng. Vui lòng thử lại.'))

    def do_POST(self):
        if not self.authorized(admin=True): return
        path = urlsplit(self.path).path
        if path not in ('/api/simulation','/api/manual'):
            return self.reply(404,dict(error='Not found'))
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<=1024: raise ValueError('Yêu cầu không hợp lệ.')
            body=json.loads(self.rfile.read(size))
            if path == '/api/manual':
                return self.reply(200,manual_visits.submit(self.server.app.daily, body))
            if not isinstance(body,dict) or set(body)!={'enabled'}: raise ValueError('Cần trường enabled.')
            self.reply(200,self.server.app.daily.set_enabled(body['enabled']))
        except manual_visits.Conflict as exc:
            self.reply(409, dict(error=str(exc)))
        except (ValueError,TimeoutError) as exc:
            self.reply(400,dict(error=str(exc)))
        except Exception:
            LOG.exception('operator request failed')
            self.reply(503,dict(error='Máy chủ chưa xác nhận. Hãy thử lại cùng thao tác.'))


def main():
    logging.basicConfig(level=logging.INFO)
    app=Application()
    server=ThreadingHTTPServer(('0.0.0.0',8088),Handler)
    server.app=app
    try:
        server.serve_forever()
    finally:
        app.stop.set()
        app.thread.join(timeout=40)
        server.server_close()


if __name__=='__main__':
    main()
