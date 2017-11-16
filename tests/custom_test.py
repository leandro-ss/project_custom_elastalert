import json
import mock
import tests
import pytest
import datetime

from custom.ruletypes import PercentileOfFieldSpikeRule

from elastalert.util import EAException
from elastalert.util import ts_now
from elastalert.util import ts_to_dt


def hits(size,time_delta=datetime.timedelta(seconds=0),**kwargs):
    ret = []
    for n in range(size):
        ts = ts_to_dt('2000-01-01T00:%s:%sZ' % (n / 60, n % 60)) + time_delta
        n += 1
        ret.append(event(ts, **kwargs))
    return ret

def event(timestamp, timestamp_field='@timestamp', **kwargs):
    event = {timestamp_field: timestamp}
    event.update(**kwargs)
    return event


RULES =  PercentileOfFieldSpikeRule({
    'name': 'PercentileOfFieldSpikeRule',
    'threshold_cur': 10,
    'spike_height': 2,
    'timeframe': datetime.timedelta(seconds=5),
    'spike_type': 'up',
    'target_field': 'cpu',
    'use_count_query': False,
    'percentile_value': 90,
    'timestamp_field': 'ts'
})

def _test1_rule():

    # Criado um evento por segundo
    events = hits(100, timestamp_field='ts', cpu=10)
    # Constant rate, doesn't match
    RULES.add_data(events)

    assert len(RULES.matches) == 0


def test2_rule():

    events1 = hits(10, time_delta=datetime.timedelta(seconds= 0), timestamp_field='ts', cpu=10)
    events2 = hits(10, time_delta=datetime.timedelta(seconds= 5), timestamp_field='ts', cpu=20)
    sorted_list = sorted(events1+events2, key=lambda data: data['ts'])

    RULES.add_data(
        sorted_list
    )

    assert len(RULES.matches) == 1


def _test2_rule():

    events1 = hits(50, time_delta=datetime.timedelta(seconds= 0), timestamp_field='ts', cpu=10)
    events2 = hits(50, time_delta=datetime.timedelta(seconds=50), timestamp_field='ts', cpu=10)
    events3 = hits(50, time_delta=datetime.timedelta(seconds=50), timestamp_field='ts', cpu=20)
    sorted_list = sorted(events1+events2+events3, key=lambda data: data['ts'])

    RULES.add_data(
        sorted_list
    )

    assert len(RULES.matches) == 1

