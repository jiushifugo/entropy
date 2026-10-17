"""Bounded, UTC-hour diagnostics; samples and executions have separate denominators."""
from collections import Counter, deque
import json
import math
import statistics
from datetime import datetime, timezone


class VolumeStats:
    def __init__(self):
        self.hour = None
        self.samples = Counter()
        self.executions = Counter()
        self.directions = {}

    def rotate(self, now):
        hour = int(now // 3600)
        previous = self.summary() if self.hour is not None and hour != self.hour else None
        if hour != self.hour:
            self.hour = hour
            self.samples.clear()
            self.executions.clear()
            self.directions.clear()
        return previous

    def summary(self):
        label = datetime.fromtimestamp(self.hour * 3600, timezone.utc).strftime('%Y-%m-%d %H:00 UTC') if self.hour is not None else 'waiting'
        blocked = sum(n for key, n in self.samples.items() if key != 'eligible')
        reasons = ', '.join(f'{key}={n} ({n / blocked:.1%})' for key, n in sorted(self.samples.items()) if key != 'eligible')
        details = json.dumps(self.direction_summary(), sort_keys=True)
        return f'{label}; blocked={blocked}; {reasons}; eligible={self.samples["eligible"]}; executions={dict(self.executions)}; directions={details}'

    def record_pair(self, direction, outcome, diagnostic, expected, realized,
                    crossing_bps=None, hedge_ms=None, hedge_drift_bps=None):
        bucket = self.directions.setdefault(direction, {
            'outcomes': Counter(), 'cancels': Counter(), 'metrics': {},
            'expected_usd': 0.0, 'realized_usd': 0.0, 'unknown_realized': 0,
        })
        bucket['outcomes'][outcome] += 1
        bucket['expected_usd'] += expected
        if realized is None:
            bucket['unknown_realized'] += 1
        else:
            bucket['realized_usd'] += realized
        reason = diagnostic.get('cancel_detail') or diagnostic.get('cancel_reason')
        if reason:
            bucket['cancels'][reason] += 1
        metrics = {key: diagnostic.get(key) for key in (
            'sign_ms', 'request_ms', 'submit_ms', 'resting_ms',
            'cancel_confirm_ms', 'total_ms')}
        metrics.update(crossing_bps=crossing_bps, hedge_ms=hedge_ms,
                       hedge_drift_bps=hedge_drift_bps)
        for key, value in metrics.items():
            if value is not None and math.isfinite(value):
                bucket['metrics'].setdefault(key, deque(maxlen=10000)).append(value)

    def direction_summary(self):
        result = {}
        for direction, bucket in self.directions.items():
            metrics = {}
            for key, values in bucket['metrics'].items():
                ordered = sorted(values)
                metrics[key] = {'n': len(ordered),
                                'median': round(statistics.median(ordered), 3),
                                'p95': round(ordered[math.ceil(.95 * len(ordered)) - 1], 3)}
            result[direction] = {
                'outcomes': dict(bucket['outcomes']), 'cancels': dict(bucket['cancels']),
                'expected_usd': round(bucket['expected_usd'], 6),
                'realized_usd': round(bucket['realized_usd'], 6),
                'unknown_realized': bucket['unknown_realized'], 'metrics': metrics,
            }
        return result

    def record_execution(self, matched, requested, tolerance, failed):
        outcome = ('error' if failed else 'no_fill' if matched <= tolerance
                   else 'full' if matched >= requested - tolerance else 'partial')
        self.executions[outcome] += 1
        return outcome
