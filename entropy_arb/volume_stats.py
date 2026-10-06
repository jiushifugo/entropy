"""Bounded, UTC-hour diagnostics; samples and executions have separate denominators."""
from collections import Counter
from datetime import datetime, timezone


class VolumeStats:
    def __init__(self):
        self.hour = None
        self.samples = Counter()
        self.executions = Counter()

    def rotate(self, now):
        hour = int(now // 3600)
        previous = self.summary() if self.hour is not None and hour != self.hour else None
        if hour != self.hour:
            self.hour = hour
            self.samples.clear()
            self.executions.clear()
        return previous

    def summary(self):
        label = datetime.fromtimestamp(self.hour * 3600, timezone.utc).strftime('%Y-%m-%d %H:00 UTC') if self.hour is not None else 'waiting'
        blocked = sum(n for key, n in self.samples.items() if key != 'eligible')
        reasons = ', '.join(f'{key}={n} ({n / blocked:.1%})' for key, n in sorted(self.samples.items()) if key != 'eligible')
        return f'{label}; blocked={blocked}; {reasons}; eligible={self.samples["eligible"]}; executions={dict(self.executions)}'

    def record_execution(self, matched, requested, tolerance, failed):
        outcome = ('error' if failed else 'no_fill' if matched <= tolerance
                   else 'full' if matched >= requested - tolerance else 'partial')
        self.executions[outcome] += 1
