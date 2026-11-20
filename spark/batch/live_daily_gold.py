"""Recompute historical daily admission/discharge aggregates from the durable event ledger."""
from spark.common.iceberg import create_spark_session


def refresh(spark):
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.gold')
    spark.sql('''CREATE TABLE IF NOT EXISTS iceberg.gold.live_department_daily_metrics (
        date DATE, department STRING, admissions BIGINT, discharges BIGINT
    ) USING iceberg PARTITIONED BY (days(date))''')
    spark.sql('''INSERT OVERWRITE iceberg.gold.live_department_daily_metrics
        SELECT to_date(from_utc_timestamp(event_time,'Asia/Ho_Chi_Minh')) AS date, department,
          SUM(CASE WHEN event_type='ADMISSION' THEN 1 ELSE 0 END) AS admissions,
          SUM(CASE WHEN event_type='DISCHARGE' THEN 1 ELSE 0 END) AS discharges
        FROM iceberg.silver.realtime_events
        WHERE event_type IN ('ADMISSION','DISCHARGE')
        GROUP BY to_date(from_utc_timestamp(event_time,'Asia/Ho_Chi_Minh')), department''')
    count=spark.table('iceberg.gold.live_department_daily_metrics').count()
    print('job=live_daily_gold output_rows=%d' % count)
    return count


if __name__=='__main__':
    session=create_spark_session('HospitalLiveDailyGold')
    session.conf.set('spark.sql.shuffle.partitions','2')
    try:
        refresh(session)
    finally:
        session.stop()
