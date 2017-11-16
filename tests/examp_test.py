import mock
import tests
import pytest
import datetime

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

#test_metric_aggregation
def _test_rule():
    rules = {'buffer_time': datetime.timedelta(minutes=5),
             'timestamp_field': '@timestamp',
             'metric_agg_type': 'avg',
             'metric_agg_key': 'cpu_pct'}

    # Check threshold logic
    with pytest.raises(EAException):
        rule = MetricAggregationRule(rules)

    rules['min_threshold'] = 0.1
    rules['max_threshold'] = 0.8

    rule = MetricAggregationRule(rules)

    assert rule.rules['aggregation_query_element'] == {'cpu_pct_avg': {'avg': {'field': 'cpu_pct'}}}

    assert rule.crossed_thresholds(None) is False
    assert rule.crossed_thresholds(0.09) is True
    assert rule.crossed_thresholds(0.10) is False
    assert rule.crossed_thresholds(0.79) is False
    assert rule.crossed_thresholds(0.81) is True

    rule.check_matches(datetime.datetime.now(), None, {'cpu_pct_avg': {'value': None}})
    rule.check_matches(datetime.datetime.now(), None, {'cpu_pct_avg': {'value': 0.5}})
    assert len(rule.matches) == 0

    rule.check_matches(datetime.datetime.now(), None, {'cpu_pct_avg': {'value': 0.05}})
    rule.check_matches(datetime.datetime.now(), None, {'cpu_pct_avg': {'value': 0.95}})
    assert len(rule.matches) == 2

    rules['query_key'] = 'qk'
    rule = MetricAggregationRule(rules)
    rule.check_matches(datetime.datetime.now(), 'qk_val', {'cpu_pct_avg': {'value': 0.95}})
    assert rule.matches[0]['qk'] == 'qk_val'