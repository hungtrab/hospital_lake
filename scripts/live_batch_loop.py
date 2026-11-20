"""Run the historical recomputation periodically; failed jobs remain visible and retry."""
import logging
import os
import subprocess
import time


def main():
    logging.basicConfig(level=logging.INFO)
    interval=int(os.environ.get('LIVE_BATCH_INTERVAL','3600'))
    if interval<60:
        raise ValueError('LIVE_BATCH_INTERVAL must be at least 60 seconds')
    time.sleep(90)
    while True:
        result=subprocess.run(['/opt/spark/bin/spark-submit','--driver-memory','1g','--conf',
            'spark.jars.ivy=/opt/ivy','--packages',
            'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0',
            'spark/batch/live_daily_gold.py'])
        logging.info('job=live_daily_gold exit_code=%d',result.returncode)
        time.sleep(interval if result.returncode==0 else 60)


if __name__=='__main__':
    main()
