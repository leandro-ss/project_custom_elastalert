# -*- coding: utf-8 -*-
"""
Rule types that count one document multiple times according to a field in the
document. This rule may only be used when the contents of the document may be
discarded (i.e. use_count_query would have sufficed), as it does not
preserve them.
"""
import copy

from elastalert.ruletypes import FrequencyRule
from elastalert.ruletypes import SpikeRule
from elastalert.ruletypes import FlatlineRule
from elastalert.ruletypes import EventWindow
from elastalert.util import lookup_es_key
from elastalert.util import elastalert_logger


def verify_integer_field(document, rule, target_field, allow_zero=False):
    count = lookup_es_key(document, target_field)
    # Attempt to convert strings to ints
    if isinstance(count, basestring):
        try:
            count = int(count)
        except ValueError:
            elastalert_logger.warning(
                'Field %s is a string which could not be converted'
                'into an integer for rule %s' % (target_field, rule['name']))
            return None
    if count is None:
        elastalert_logger.warning(
            'Did not find field %s representing document count in '
            'document for rule %s' % (target_field, rule['name']))
        return None
    if isinstance(count, float):
        count = int(count)
    if not isinstance(count, (int, long)):
        elastalert_logger.warning(
            'Non-integer value of field %s representing document '
            'count in document for rule %s: %s'
            % (target_field, rule['name'], count))
        return None
    if count < 0 or (count == 0 and not allow_zero):
        return None
    return count


class SumOfFieldRuleFactory(type):
    """
    Metaclass that creates a subclass of the class's backing_rule_type.
    backing_rule_type must implement add_count_data.
    """
    # This allows the add_data to use super(...).add_count_data(...) to call
    # the add_count_data method of the appropriate backing RuleType, without
    # having to reimplement all the common logic for each backing_rule_type.

    def __new__(mcs, name, _, dct):

        def add_data(self, data):
            for document in data:
                count = verify_integer_field(
                    document, self.rules, self.rules['target_field'])
                ts = lookup_es_key(document, self.ts_field)
                if count and ts:
                    super(self.__class__, self).add_count_data({ts: count})

        backing_rule_type_cls = dct['backing_rule_type']
        dct['required_options'] = (
            dct['backing_rule_type'].required_options | frozenset(['target_field']))
        dct['add_data'] = add_data
        return type.__new__(mcs, name, (backing_rule_type_cls,), dct)


class SumOfFieldFrequencyRule(object):
    __metaclass__ = SumOfFieldRuleFactory
    backing_rule_type = FrequencyRule


class SumOfFieldSpikeRule(object):
    __metaclass__ = SumOfFieldRuleFactory
    backing_rule_type = SpikeRule
 

class BaseAggregationRule(RuleType):
    def __init__(self, *args):
        super(BaseAggregationRule, self).__init__(*args)
        bucket_interval = self.rules.get('bucket_interval')
        if bucket_interval:
            if 'seconds' in bucket_interval:
                self.rules['bucket_interval_period'] = str(bucket_interval['seconds']) + 's'
            elif 'minutes' in bucket_interval:
                self.rules['bucket_interval_period'] = str(bucket_interval['minutes']) + 'm'
            elif 'hours' in bucket_interval:
                self.rules['bucket_interval_period'] = str(bucket_interval['hours']) + 'h'
            elif 'days' in bucket_interval:
                self.rules['bucket_interval_period'] = str(bucket_interval['days']) + 'd'
            elif 'weeks' in bucket_interval:
                self.rules['bucket_interval_period'] = str(bucket_interval['weeks']) + 'w'
            else:
                raise EAException("Unsupported window size")

            if self.rules.get('use_run_every_query_size'):
                if total_seconds(self.rules['run_every']) % total_seconds(self.rules['bucket_interval_timedelta']) != 0:
                    raise EAException("run_every must be evenly divisible by bucket_interval if specified")
            else:
                if total_seconds(self.rules['buffer_time']) % total_seconds(self.rules['bucket_interval_timedelta']) != 0:
                    raise EAException("Buffer_time must be evenly divisible by bucket_interval if specified")

    def generate_aggregation_query(self):
        raise NotImplementedError()

    def add_aggregation_data(self, payload):
        for timestamp, payload_data in payload.iteritems():
            if 'interval_aggs' in payload_data:
                self.unwrap_interval_buckets(timestamp, None, payload_data['interval_aggs']['buckets'])
            elif 'bucket_aggs' in payload_data:
                self.unwrap_term_buckets(timestamp, payload_data['bucket_aggs']['buckets'])
            else:
                self.check_matches(timestamp, None, payload_data)

    def unwrap_interval_buckets(self, timestamp, query_key, interval_buckets):
        for interval_data in interval_buckets:
            # Use bucket key here instead of start_time for more accurate match timestamp
            self.check_matches(ts_to_dt(interval_data['key_as_string']), query_key, interval_data)

    def unwrap_term_buckets(self, timestamp, term_buckets):
        for term_data in term_buckets:
            if 'interval_aggs' in term_data:
                self.unwrap_interval_buckets(timestamp, term_data['key'], term_data['interval_aggs']['buckets'])
            else:
                self.check_matches(timestamp, term_data['key'], term_data)

    def check_matches(self, timestamp, query_key, aggregation_data):
        raise NotImplementedError()



class MetricAggregationRule(BaseAggregationRule):
    """ A rule that matches when there is a low number of events given a timeframe. """
    required_options = frozenset(['metric_agg_key', 'metric_agg_type', 'doc_type'])
    allowed_aggregations = frozenset(['min', 'max', 'avg', 'sum', 'cardinality', 'value_count'])

    def __init__(self, *args):
        super(MetricAggregationRule, self).__init__(*args)
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        if 'max_threshold' not in self.rules and 'min_threshold' not in self.rules:
            raise EAException("MetricAggregationRule must have at least one of either max_threshold or min_threshold")

        self.metric_key = self.rules['metric_agg_key'] + '_' + self.rules['metric_agg_type']

        if not self.rules['metric_agg_type'] in self.allowed_aggregations:
            raise EAException("metric_agg_type must be one of %s" % (str(self.allowed_aggregations)))

        self.rules['aggregation_query_element'] = self.generate_aggregation_query()

    def get_match_str(self, match):
        message = 'Threshold violation, %s:%s %s (min: %s max : %s) \n\n' % (
            self.rules['metric_agg_type'],
            self.rules['metric_agg_key'],
            match[self.metric_key],
            self.rules.get('min_threshold'),
            self.rules.get('max_threshold')
        )
        return message

    def generate_aggregation_query(self):
        return {self.metric_key: {self.rules['metric_agg_type']: {'field': self.rules['metric_agg_key']}}}

    def check_matches(self, timestamp, query_key, aggregation_data):
        metric_val = aggregation_data[self.metric_key]['value']
        if self.crossed_thresholds(metric_val):
            match = {self.rules['timestamp_field']: timestamp,
                     self.metric_key: metric_val}
            if query_key is not None:
                match[self.rules['query_key']] = query_key
            self.add_match(match)

    def crossed_thresholds(self, metric_value):
        if metric_value is None:
            return False
        if 'max_threshold' in self.rules and metric_value > self.rules['max_threshold']:
            return True
        if 'min_threshold' in self.rules and metric_value < self.rules['min_threshold']:
            return True
        return False

