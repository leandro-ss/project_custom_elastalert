import mock
import tests
import pytest
import datetime

from custom.ruletypes import PercentileOfFieldSpikeRule

from elastalert.util import EAException
from elastalert.util import ts_now
from elastalert.util import ts_to_dt


#custom.custom_rule2.SpikeAggregationRule
def hits(size, **kwargs):
    ret = []
    for n in range(size):
        ts = ts_to_dt('2000-01-01T00:%s:%sZ' % (n / 60, n % 60))
        n += 1
        event = create_event(ts, **kwargs)
        ret.append(event)
    return ret


def create_event(timestamp, timestamp_field='@timestamp', **kwargs):
    event = {timestamp_field: timestamp}
    event.update(**kwargs)
    return event


def test_rule():

    # Events are 1 per second
    events = hits(100, timestamp_field='ts', cpu=10)
    # Constant rate, doesn't match
    rules = {
             'name': 'PercentileOfFieldSpikeRule',
             'threshold_ref': 10,
             'spike_height': 2,
             'timeframe': datetime.timedelta(seconds=10),
             'spike_type': 'both',
             'target_field': 'cpu',
             'use_count_query': False, # Comportamento nao apresentou diferenca
             'percentile_value': 100,
             'timestamp_field': 'ts'}
    rule = PercentileOfFieldSpikeRule(rules)
    rule.add_data(events)
    #assert len(rule.matches) == 0

    # Double the rate of events after [50:]
    # Adcionar os primeiros 50 eventos
    events2 = events[:50]
    for event in events[50:]:
        # Adcionar os ultimos 50 eventos
        events2.append(event)
        # Adcionar os ultimos 50 eventos novamente acrecidos de 10 segundos sobre ts e 10 pontos sobre a CPU
        events2.append({'ts': event['ts'] + datetime.timedelta(seconds=10) , 'cpu':20})

    rule = PercentileOfFieldSpikeRule(rules)
    rule.add_data(events2)
    #print (rule.matches)
    assert len(rule.matches) == 1
