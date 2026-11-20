"""Summarize a synthetic Bạch Mai arrival cohort offline using the live scheduler."""
import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sys
from statistics import mean

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generators.stream_generator import provincial


def summarize(start, days, visits, seed):
    free = {}
    codes = Counter()
    daily = {'weekday': [], 'weekend': []}
    arrivals, admissions, ages, conditions = Counter(), Counter(), Counter(), Counter()
    for offset in range(days):
        day = start + timedelta(days=offset)
        count = 0
        for visit, due in provincial.schedule(day.isoformat(), visits, seed):
            count += 1
            arrivals[datetime.fromtimestamp(due, provincial.LOCAL).hour] += 1
            events = provincial.journey(visit, due, seed, free)
            patient = events[0]['payload']
            age = patient['age_years']
            band = 'under_18' if age < 18 else '18_to_59' if age < 60 else '60_plus'
            ages[(patient['care_setting'], band)] += 1
            conditions[patient['condition_group']] += 1
            if 'diagnosis_code' in patient:
                codes[patient['diagnosis_code']] += 1
            for event in events:
                if event['event_type'] == 'ADMISSION':
                    at = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00'))
                    admissions[at.astimezone(provincial.LOCAL).hour] += 1
        daily['weekend' if day.weekday() >= 5 else 'weekday'].append(count)
    return dict(start=start.isoformat(), days=days, seed=seed, visits=sum(arrivals.values()),
                mean_per_day={k: mean(v) if v else None for k, v in daily.items()},
                arrivals_by_hour=[arrivals[h] for h in range(24)],
                admissions_by_hour=[admissions[h] for h in range(24)],
                ages={setting: {band: ages[setting, band] for band in ('under_18', '18_to_59', '60_plus')}
                      for setting in ('emergency', 'outpatient')},
                condition_groups=dict(conditions.most_common()), diagnosis_codes=dict(codes.most_common()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=date.fromisoformat, default=date(2026,11,23))
    parser.add_argument('--days', type=int, default=28)
    parser.add_argument('--visits', type=int, default=5315)
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    if args.days < 1 or args.visits < 1:
        parser.error('--days and --visits must be positive')
    print(json.dumps(summarize(args.start, args.days, args.visits, args.seed), ensure_ascii=False, indent=2))
