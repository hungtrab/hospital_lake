from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT/'data/samples/overnight_journey.json'
SPEC = importlib.util.spec_from_file_location('journey_verifier', ROOT/'scripts/verify_patient_journey.py')
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
compare_journey = MODULE.compare_journey


def observed():
    # Fixed observation values from the contract, independent of the fixture.
    values = [('admission',1,1,0), ('midnight',1,1,0), ('transfer',1,0,1),
              ('late_replay',1,0,1), ('discharge',0,0,0), ('discharge_replay',0,0,0)]
    return dict(scenario_id='overnight-v1', steps=[
        dict(step_id=s,current_patients=n,by_department=dict(ICU=i,CARDIOLOGY=c))
        for s,n,i,c in values])


class JourneyTests(unittest.TestCase):
    def setUp(self):
        self.expected = json.loads(FIXTURE.read_text())

    def test_fixture_and_reordered_observations(self):
        actual = observed()
        actual['steps'].reverse()
        self.assertEqual(compare_journey(self.expected, actual), dict(status='PASS',checked_steps=6))
        steps = self.expected['steps']
        self.assertEqual(steps[0]['events'], steps[3]['events'])
        self.assertEqual(steps[4]['events'], steps[5]['events'])
        self.assertEqual(steps[1]['events'], [])
        self.assertEqual(steps[1]['as_of'], '2026-11-23T17:05:00Z')

    def test_wrong_count_is_a_mismatch_not_schema_error(self):
        actual = observed()
        actual['steps'][1]['current_patients'] = 0
        result = compare_journey(self.expected, actual)
        self.assertEqual(result['status'], 'FAIL')
        self.assertEqual(result['mismatches'], [dict(step_id='midnight',field='current_patients',expected=1,actual=0)])

    def test_department_mismatch_report(self):
        actual = observed()
        actual['steps'][2]['by_department'] = dict(ICU=1,CARDIOLOGY=0)
        result = compare_journey(self.expected, actual)
        self.assertEqual(len(result['mismatches']), 2)
        self.assertEqual({m['field'] for m in result['mismatches']}, {'by_department.ICU','by_department.CARDIOLOGY'})

    def test_invalid_observations_rejected(self):
        invalid = []
        value = observed(); value['steps'].pop(); invalid.append(value)
        value = observed(); value['steps'].append(deepcopy(value['steps'][0])); invalid.append(value)
        value = observed(); value['steps'][0]['step_id'] = 'extra'; invalid.append(value)
        value = observed(); value['scenario_id'] = 'wrong'; invalid.append(value)
        value = observed(); del value['steps'][0]['by_department']['ICU']; invalid.append(value)
        value = observed(); value['steps'][0]['by_department']['OTHER'] = 0; invalid.append(value)
        for count in [True, -1, 1.0, '1', None]:
            value = observed(); value['steps'][0]['current_patients'] = count; invalid.append(value)
        invalid.extend([None, {}, dict(scenario_id='overnight-v1',steps=[])])
        for actual in invalid:
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                compare_journey(self.expected, actual)

    def test_bad_expected_rejected(self):
        for mutation in ['count','as_of','date_only','events','replay']:
            fixture = deepcopy(self.expected)
            if mutation == 'count': fixture['steps'][0]['expected']['current_patients'] = True
            if mutation == 'as_of': fixture['steps'][0]['as_of'] = 'bad'
            if mutation == 'date_only': fixture['steps'][0]['as_of'] = '2026-11-23Z'
            if mutation == 'events': fixture['steps'][0]['events'] = None
            if mutation == 'replay': fixture['steps'][3]['events'][0]['payload']['department'] = 'CARDIOLOGY'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                compare_journey(fixture, observed())

    def test_cli_exit_codes(self):
        with tempfile.TemporaryDirectory() as folder:
            actual_path = Path(folder)/'actual.json'
            command = [sys.executable, '-S', str(ROOT/'scripts/verify_patient_journey.py'),
                       '--expected', str(FIXTURE), '--actual', str(actual_path)]
            cases = [(json.dumps(observed()), 0, 'PASS')]
            wrong = observed(); wrong['steps'][1]['current_patients'] = 0
            cases += [(json.dumps(wrong),1,'FAIL'), ('{bad',2,None),
                      ('{"scenario_id":"x","scenario_id":"y","steps":[]}',2,None)]
            for content, code, status in cases:
                actual_path.write_text(content, encoding='utf-8')
                result = subprocess.run(command,capture_output=True,text=True,cwd=folder)
                self.assertEqual(result.returncode,code,result.stderr)
                if status:
                    self.assertEqual(json.loads(result.stdout)['status'],status)
                else:
                    self.assertTrue(result.stderr)
                    self.assertEqual(result.stdout,'')
            invalid_fixture = deepcopy(self.expected)
            invalid_fixture['steps'][0]['as_of'] = '2026-11-23Z'
            expected_path = Path(folder)/'bad-expected.json'
            expected_path.write_text(json.dumps(invalid_fixture),encoding='utf-8')
            actual_path.write_text(json.dumps(observed()),encoding='utf-8')
            command[command.index('--expected')+1] = str(expected_path)
            result = subprocess.run(command,capture_output=True,text=True,cwd=folder)
            self.assertEqual(result.returncode,2)
            self.assertNotIn('Traceback',result.stderr)
            self.assertIn('as_of',result.stderr)


if __name__ == '__main__':
    unittest.main()
