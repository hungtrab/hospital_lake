import json
from unittest.mock import patch

import pytest

from scripts import k8s


def observation(state, submission='new', executors=2):
    return json.dumps({'status': {'applicationState': {'state': state},
                                  'submissionID': submission,
                                  'executorState': {f'exec-{n}': 'RUNNING' for n in range(executors)}}})


def test_scale_waits_for_replacement_submission_and_requested_executors():
    responses=[observation('RUNNING','old',2), observation('RUNNING','new',2),
               observation('RUNNING','new',4)]
    with patch.object(k8s,'kubectl',side_effect=responses) as get, patch.object(k8s.time,'sleep'):
        k8s.wait_application('hospital-stream','RUNNING',previous_submission='old',expected_executors=4)
    assert get.call_count==3


def test_submission_retries_are_not_terminal_but_failed_is():
    with patch.object(k8s,'kubectl',side_effect=[observation('SUBMISSION_FAILED'),observation('RUNNING')]), \
            patch.object(k8s.time,'sleep'):
        k8s.wait_application('hospital-stream','RUNNING')
    with patch.object(k8s,'kubectl',return_value=observation('FAILED')):
        with pytest.raises(RuntimeError,match='FAILED'):
            k8s.wait_application('hospital-stream','RUNNING')


def test_stale_status_does_not_wait_forever():
    with patch.object(k8s,'kubectl',return_value=observation('RUNNING','old')), \
            patch.object(k8s.time,'monotonic',side_effect=[0,0,2]),patch.object(k8s.time,'sleep'):
        with pytest.raises(TimeoutError):
            k8s.wait_application('hospital-stream','RUNNING',timeout=1,previous_submission='old')


def test_defaulted_fields_do_not_force_an_unnecessary_resubmission():
    desired={'executor':{'instances':2},'deps':{'packages':['one']}}
    actual={'executor':{'instances':2,'cores':1},'deps':{'packages':['one']},'other':'default'}
    assert k8s.spec_matches(actual,desired)
    assert not k8s.spec_matches(actual,dict(desired,executor={'instances':4}))
