import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('week_report', ROOT/'scripts/department_week_report.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
build_report = MODULE.build_report
CSV = '''date,department,admissions
2026-11-08,ICU,999
2026-11-09,ICU,2
2026-11-15,ICU,2
2026-11-16,ICU,3
2026-11-22,ICU,3
2026-11-16,CARDIOLOGY,3
2026-11-10,EMERGENCY,2
2026-11-20,SURGERY,0
2026-11-23,ICU,999
'''
EXPECTED = [
    dict(department='CARDIOLOGY',previous_admissions=0,current_admissions=3,delta=3,change_pct=None),
    dict(department='EMERGENCY',previous_admissions=2,current_admissions=0,delta=-2,change_pct=-100.0),
    dict(department='ICU',previous_admissions=4,current_admissions=6,delta=2,change_pct=50.0),
    dict(department='SURGERY',previous_admissions=0,current_admissions=0,delta=0,change_pct=None)]


class WeekReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.input = self.directory/'input.csv'

    def report(self, text, as_of='2026-11-23'):
        self.input.write_text(text,encoding='utf-8')
        return build_report(self.input,as_of)

    def test_exact_fixture_and_excluded_department(self):
        self.assertEqual(self.report(CSV+'2026-11-08,OUTSIDE,17\n'), EXPECTED)

    def test_empty_input(self):
        for text in ['', 'date,department,admissions\n']:
            self.assertEqual(self.report(text), [])

    def test_fractional_percentage_and_month_boundary(self):
        result = self.report('date,department,admissions\n2026-10-25,ICU,3\n2026-11-01,ICU,4\n',
                             as_of='2026-11-08')
        self.assertEqual(result[0]['change_pct'], 33.33)
        self.assertEqual(result[0]['delta'], 1)

    def test_bad_csv_rejected(self):
        rows = ['2026-11-20,ICU,-1', '2026-11-20,ICU,1.0', '2026-11-20,ICU,true',
                '2026-11-20,ICU,9223372036854775808','2026-02-30,ICU,1',
                '20261120,ICU,1','2026-11-20,icu,1','2026-11-20,,1',
                '2026-11-20,ICU,1,extra','2026-11-20,ICU',
                '2026-11-20,ICU,1\n2026-11-20,ICU,2']
        for row in rows:
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.report('date,department,admissions\n'+row+'\n')
        with self.assertRaises(ValueError): self.report('department,date,admissions\n')
        for as_of in ['20261123','2026-02-30','0001-01-01']:
            with self.subTest(as_of=as_of), self.assertRaises(ValueError):
                self.report(CSV,as_of)

    def test_runner_executes_supplied_sql_file(self):
        sql = self.directory/'marker.sql'
        sql.write_text('SELECT SUM(admissions) AS sentinel FROM department_daily_metrics',encoding='utf-8')
        with patch.object(MODULE,'SQL_PATH',sql):
            self.assertEqual(self.report(CSV),[dict(sentinel=2013)])

    def test_cli_output_and_failure_preserves_previous_file(self):
        self.input.write_text(CSV,encoding='utf-8')
        output = self.directory/'report.json'
        command = [sys.executable,'-S',str(ROOT/'scripts/department_week_report.py'),
                   '--input',str(self.input),'--as-of','2026-11-23','--output',str(output)]
        result = subprocess.run(command,capture_output=True,text=True,cwd=self.directory)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(output.read_text()),EXPECTED)
        before = output.read_bytes()
        self.input.write_text('date,department,admissions\n2026-11-20,ICU,-1\n',encoding='utf-8')
        result = subprocess.run(command,capture_output=True,text=True,cwd=self.directory)
        self.assertEqual(result.returncode,2)
        self.assertTrue(result.stderr)
        self.assertEqual(output.read_bytes(),before)
        self.assertFalse(list(self.directory.glob('.week-report-*')))


if __name__ == '__main__':
    unittest.main()
