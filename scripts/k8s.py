"""Reproducible single-node kind demo; never changes the user's kubeconfig/context."""
from __future__ import annotations
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml
from scripts.grafana_smoke import env_file
from scripts.k8s_tools import BIN, ROOT

CLUSTER = os.environ.get('HOSPITAL_KIND_CLUSTER', 'hospital-lake-lab')
if not re.fullmatch(r'hospital-lake-[a-z0-9-]{1,32}', CLUSTER):
    raise ValueError('expected a hospital-lake-* demo cluster name')
NAMESPACE = 'hospital-lake'
STATE = ROOT/'results'/('k8s' if CLUSTER == 'hospital-lake-lab' else CLUSTER)
KUBECONFIG = STATE/'kubeconfig'
IMAGES = ['hospital-lake/minio:RELEASE.2025-04-22T22-12-26Z', 'hospital-lake/iceberg-rest:1.9.0',
          'hospital-lake/spark:completion', 'trinodb/trino:483', 'clickhouse/clickhouse-server:25.8.18.1',
          'grafana/grafana:12.2.0', 'prom/prometheus:v3.13.5']


def run(command, *, body=None, capture=False):
    return subprocess.run([str(x) for x in command], input=body, text=True, check=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def kubectl(*args, body=None, capture=False):
    return run([BIN/'kubectl', '--kubeconfig', KUBECONFIG, '--context', 'kind-'+CLUSTER,
                '-n', NAMESPACE, *args], body=body, capture=capture)


def apply(objects):
    kubectl('apply', '-f', '-', body=yaml.safe_dump_all(objects, sort_keys=False))


def configmap(name, data):
    apply([dict(apiVersion='v1', kind='ConfigMap', metadata=dict(name=name, namespace=NAMESPACE), data=data)])


def configure(settings):
    config = dict(MINIO_ENDPOINT='http://minio:9000', MINIO_REGION='us-east-1',
        ICEBERG_CATALOG_URI='http://iceberg-rest:8181', ICEBERG_WAREHOUSE='s3://warehouse/',
        KAFKA_BOOTSTRAP_SERVERS='hospital-kafka-bootstrap:9092', KAFKA_TOPICS='hospital.events',
        KAFKA_STARTING_OFFSETS='earliest', CLICKHOUSE_URL='http://clickhouse:8123', CLICKHOUSE_USER='hospital_writer',
        GRAFANA_URL='http://grafana:3000', TRINO_URL='http://trino:8080',
        SPARK_MASTER='k8s://https://kubernetes.default.svc:443', REALTIME_CHECKPOINT_LOCATION='/state/checkpoints/realtime',
        REALTIME_ICU_BEDS=settings['REALTIME_ICU_BEDS'], STREAM_MAX_OFFSETS=settings['STREAM_MAX_OFFSETS'],
        STREAM_TRIGGER=settings['STREAM_TRIGGER'], GOLD_AS_OF=settings['GOLD_AS_OF'],
        GOLD_BED_CAPACITIES=settings['GOLD_BED_CAPACITIES'])
    configmap('hospital-config', config)
    keys=['MINIO_ACCESS_KEY','MINIO_SECRET_KEY','CLICKHOUSE_PASSWORD','CLICKHOUSE_ADMIN_PASSWORD',
          'CLICKHOUSE_GRAFANA_PASSWORD','GRAFANA_ADMIN_USER','GRAFANA_ADMIN_PASSWORD']
    apply([dict(apiVersion='v1',kind='Secret',metadata=dict(name='hospital-secrets',namespace=NAMESPACE),
                type='Opaque',stringData={key:settings[key] for key in keys})])
    configmap('minio-bootstrap', {'bootstrap.sh':(ROOT/'infra/docker/minio/bootstrap.sh').read_text()})
    configmap('clickhouse-bootstrap', {'bootstrap.sh':(ROOT/'infra/docker/clickhouse/bootstrap.sh').read_text(),
                                      'schema.sql':(ROOT/'sql/clickhouse/live_schema.sql').read_text()})
    configmap('clickhouse-metrics', {'prometheus.xml':(ROOT/'infra/k8s/clickhouse/prometheus.xml').read_text()})
    configmap('kafka-metrics', {'metrics.yaml':'lowercaseOutputName: true\nrules:\n  - pattern: ".*"\n'})
    trino=ROOT/'infra/docker/trino'
    configmap('trino-config', {'config.properties':(trino/'config.properties').read_text(),
                              'jvm.config':(trino/'jvm.config').read_text(),
                              'iceberg.properties':(trino/'catalog/iceberg.properties').read_text()})
    grafana=ROOT/'dashboard/grafana'
    configmap('grafana-datasources', {'hospital.yaml':(grafana/'provisioning/datasources/hospital.yaml').read_text()})
    configmap('grafana-provider', {'hospital.yaml':(grafana/'provisioning/dashboards/hospital.yaml').read_text()})
    configmap('grafana-dashboards', {p.name:p.read_text() for p in (grafana/'dashboards').glob('*.json')})
    configmap('prometheus-config', {'prometheus.yml':(ROOT/'infra/k8s/prometheus/prometheus.yml').read_text()})


def bootstrap_job(name, image, files):
    kubectl('delete','job',name,'--ignore-not-found=true','--wait=true')
    volume=dict(name='bootstrap',configMap=dict(name=name))
    mounts=[dict(name='bootstrap',mountPath='/'+key,subPath=key) for key in files]
    container=dict(name='bootstrap',image=image,command=['/bin/sh','/bootstrap.sh'],volumeMounts=mounts,
                   envFrom=[{'configMapRef':{'name':'hospital-config'}},{'secretRef':{'name':'hospital-secrets'}}])
    job=dict(apiVersion='batch/v1',kind='Job',metadata=dict(name=name,namespace=NAMESPACE),
             spec=dict(backoffLimit=2,template=dict(spec=dict(restartPolicy='Never',containers=[container],volumes=[volume]))))
    apply([job])
    try:
        kubectl('wait','--for=condition=complete','job/'+name,'--timeout=300s')
    except subprocess.CalledProcessError:
        kubectl('logs','job/'+name)
        raise


def create_cluster():
    STATE.mkdir(parents=True,exist_ok=True)
    clusters=run([BIN/'kind','get','clusters'],capture=True).splitlines()
    if CLUSTER not in clusters:
        run([BIN/'kind','create','cluster','--name',CLUSTER,'--config',ROOT/'infra/k8s/kind.yaml',
             '--kubeconfig',KUBECONFIG,'--wait','120s'])
    else:
        run(['docker','start',CLUSTER+'-control-plane'])
        KUBECONFIG.write_text(run([BIN/'kind','get','kubeconfig','--name',CLUSTER],capture=True))
    KUBECONFIG.chmod(0o600)
    deadline = time.monotonic() + 120
    while True:
        try:
            if kubectl('get', '--raw=/readyz', capture=True).strip() == 'ok':
                break
        except subprocess.CalledProcessError:
            pass  # A resumed kind node starts its API server asynchronously.
        if time.monotonic() >= deadline:
            raise TimeoutError('kind API server did not become ready')
        time.sleep(2)
    kubectl('apply','-f',ROOT/'infra/k8s/namespace.yaml')


def operators():
    helm=[BIN/'helm','--kubeconfig',KUBECONFIG,'--kube-context','kind-'+CLUSTER]
    for name,repo,chart,version,values in [
        ('strimzi','https://strimzi.io/charts/','strimzi-kafka-operator','0.48.0','strimzi-values.yaml'),
        ('spark-operator','https://kubeflow.github.io/spark-operator','spark-operator','2.4.0','spark-operator-values.yaml')]:
        run([*helm,'upgrade','--install',name,chart,'--repo',repo,'--version',version,
             '--namespace','hospital-system','--create-namespace','-f',ROOT/'infra/helm'/values,
             '--wait','--timeout','10m'])


def deploy(settings):
    docker_root=run(['docker','info','--format','{{.DockerRootDir}}'],capture=True).strip()
    if shutil.disk_usage(docker_root).free < 10*1024**3:
        raise RuntimeError('Kubernetes image import needs at least 10 GiB free in Docker storage; free space before deploying')
    create_cluster()
    for image in IMAGES:
        if subprocess.run(['docker','image','inspect',image],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
            run(['docker','pull',image])
    run([BIN/'kind','load','docker-image','--name',CLUSTER,*IMAGES])
    configure(settings)
    operators()
    kubectl('apply','-f',ROOT/'infra/k8s/minio/platform.yaml')
    kubectl('rollout','status','statefulset/minio','--timeout=300s')
    bootstrap_job('minio-bootstrap',IMAGES[0],['bootstrap.sh'])
    kubectl('apply','-f',ROOT/'infra/k8s/prometheus/rbac.yaml')
    for component in ['iceberg','trino','clickhouse','grafana','prometheus','kafka']:
        kubectl('apply','-f',ROOT/f'infra/k8s/{component}/platform.yaml')
    kubectl('apply','-f',ROOT/'infra/k8s/spark/storage.yaml')
    kubectl('apply','-f',ROOT/'infra/k8s/kafka/producer.yaml')
    for kind,name in [('statefulset','iceberg-rest'),('statefulset','clickhouse'),('deployment','trino'),
                      ('deployment','grafana'),('deployment','prometheus')]:
        kubectl('rollout','status',kind+'/'+name,'--timeout=600s')
    bootstrap_job('clickhouse-bootstrap',IMAGES[4],['bootstrap.sh','schema.sql'])
    kubectl('wait','kafka/hospital','--for=condition=Ready','--timeout=600s')
    kubectl('wait','kafkatopic/hospital.events','kafkatopic/hospital.dlq','--for=condition=Ready','--timeout=300s')


def spec_matches(actual, desired):
    """Ignore API defaulted fields, but compare every requested field."""
    if isinstance(desired, dict):
        return isinstance(actual, dict) and all(spec_matches(actual.get(k), v) for k,v in desired.items())
    if isinstance(desired, list):
        return isinstance(actual, list) and len(actual)==len(desired) and all(
            spec_matches(a,b) for a,b in zip(actual,desired))
    return actual==desired


def wait_application(name, desired, timeout=1200, *, previous_submission=None, expected_executors=None):
    deadline=time.monotonic()+timeout
    previous=None
    while time.monotonic()<deadline:
        app=json.loads(kubectl('get','sparkapplication',name,'-o','json',capture=True))
        observed=app.get('status',{})
        status=observed.get('applicationState',{})
        state=status.get('state','PENDING')
        if state!=previous:
            print(f'{name}: {state}',flush=True)
            previous=state
        fresh=(previous_submission is None or
               bool(observed.get('submissionID')) and observed['submissionID']!=previous_submission)
        running=sum(value=='RUNNING' for value in observed.get('executorState',{}).values())
        if fresh and state==desired and (expected_executors is None or running>=expected_executors):return
        # SUBMISSION_FAILED is retried by the operator; FAILED is terminal.
        if fresh and state=='FAILED':
            raise RuntimeError(f'{name}: {status}')
        time.sleep(5)
    raise TimeoutError(f'{name} did not reach {desired}')


def smoke():
    name='hospital-realtime-smoke'
    kubectl('delete','job',name,'--ignore-not-found=true','--wait=true')
    container=dict(name='smoke',image=IMAGES[2],imagePullPolicy='IfNotPresent',
        command=['python3','-m','scripts.realtime_smoke','--timeout','300','--output','/state/results/k8s-realtime.json'],
        envFrom=[{'configMapRef':{'name':'hospital-config'}},{'secretRef':{'name':'hospital-secrets'}}],
        volumeMounts=[dict(name='state',mountPath='/state')])
    spec=dict(restartPolicy='Never',securityContext={'fsGroup':185},containers=[container],
              volumes=[dict(name='state',persistentVolumeClaim={'claimName':'spark-state'})])
    apply([dict(apiVersion='batch/v1',kind='Job',metadata=dict(name=name,namespace=NAMESPACE),
                spec=dict(backoffLimit=0,template=dict(spec=spec)))])
    try:
        kubectl('wait','--for=condition=complete','job/'+name,'--timeout=900s')
    finally:
        kubectl('logs','job/'+name)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['create','deploy','status','batch','stream','smoke','produce','stop-producer','stop','forward'])
    parser.add_argument('--env-file',type=Path,default=ROOT/'.env')
    parser.add_argument('--patients',type=int,default=1000)
    parser.add_argument('--executors',type=int,default=2)
    parser.add_argument('--rate',type=int,default=100)
    parser.add_argument('--service',default='grafana')
    parser.add_argument('--port',type=int,default=3000)
    parser.add_argument('--local-port',type=int,help='host port for forward; defaults to --port')
    args=parser.parse_args()
    if args.patients < 1 or args.executors < 1 or args.rate < 1:
        parser.error('patients, executors and rate must be positive')
    if not all((BIN/name).is_file() for name in ['kind','kubectl','helm']):
        parser.error('run python -m scripts.k8s_tools first')
    settings={**env_file(ROOT/'.env.example'),**env_file(args.env_file)}
    if args.action=='create':create_cluster()
    elif args.action=='deploy':deploy(settings)
    elif args.action=='status':kubectl('get','pods,svc,pvc,sparkapplications')
    elif args.action in ('batch','stream'):
        name='hospital-'+args.action
        obj=yaml.safe_load((ROOT/f'infra/k8s/spark/{name}.yaml').read_text())
        image_id = run(['docker','image','inspect','--format','{{.Id}}',obj['spec']['image']],capture=True).strip()
        for role in ('driver','executor'):
            obj['spec'][role]['labels']['hospital-image-revision'] = image_id.split(':')[-1][:32]
        obj['spec']['executor']['instances']=args.executors
        previous_submission=None
        if args.action=='batch':
            obj['spec']['arguments']=['--patients',str(args.patients),'--seed','42','--batch-id',f'k8s-demo-{args.patients}-42']
            kubectl('delete','sparkapplication',name,'--ignore-not-found=true','--wait=true')
        else:
            current=kubectl('get','sparkapplication',name,'--ignore-not-found=true','-o','json',capture=True)
            if current:
                current=json.loads(current)
                if not spec_matches(current.get('spec',{}),obj['spec']):
                    previous_submission=current.get('status',{}).get('submissionID','')
        # Applying an existing SparkApplication spec lets its operator handle
        # resubmission; checkpoints/state are deliberately preserved.
        apply([obj])
        wait_application(name,'COMPLETED' if args.action=='batch' else 'RUNNING',
                         previous_submission=previous_submission,
                         expected_executors=args.executors if args.action=='stream' else None)
    elif args.action=='smoke':smoke()
    elif args.action=='produce':
        kubectl('patch','deployment','stream-generator','--type=strategic','-p',json.dumps({'spec':{'replicas':1,
            'template':{'spec':{'containers':[{'name':'stream-generator','args':['--rate',str(args.rate),'--duration','3600']}]}}}}))
    elif args.action=='stop-producer':kubectl('scale','deployment/stream-generator','--replicas=0')
    elif args.action=='stop':run(['docker','stop',CLUSTER+'-control-plane'])
    elif args.action=='forward':kubectl('port-forward','service/'+args.service,f'{args.local_port or args.port}:{args.port}')


if __name__=='__main__':
    main()
