"""Run Iceberg maintenance in Compose and save its measured JSON report."""
import argparse
import json
import os
import shlex
import subprocess
from pathlib import Path


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table',default='iceberg.bronze.stream_events_raw')
    parser.add_argument('--target-mb',type=int,default=128)
    parser.add_argument('--output',type=Path,default=Path('results/compaction.json'))
    args=parser.parse_args()
    command=[*shlex.split(os.environ.get('COMPOSE','docker compose')),'run','--rm','maintenance',
             '--table',args.table,'--target-mb',str(args.target_mb)]
    result=subprocess.run(command,stdout=subprocess.PIPE,text=True,check=True)
    reports=[json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    report=next((row for row in reversed(reports) if row.get('job')=='iceberg_compaction'),None)
    if report is None:
        raise RuntimeError('maintenance job returned no measurement report')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':
    main()
