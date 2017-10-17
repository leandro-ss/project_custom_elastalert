from elastalert.ruletypes import BaseAggregationRule, EventWindow
from elastalert.util import pretty_ts, new_get_event_ts, EAException, elastalert_logger

class SpikeAggregationRule(BaseAggregationRule):
    required_options = frozenset(['metric_agg_key', 'metric_agg_type', 'doc_type', 'timeframe', 'spike_height', 'spike_type'])
    allowed_aggregations = frozenset(['min', 'max', 'avg', 'sum', 'cardinality', 'value_count'])

    def __init__(self, *args):
        super(SpikeAggregationRule, self).__init__(*args)

        # shared setup
        self.ts_field = self.rules.get('timestamp_field', '@timestamp')

        # aggregation setup

        self.metric_key = self.rules['metric_agg_key'] + '_' + self.rules['metric_agg_type']

        if not self.rules['metric_agg_type'] in self.allowed_aggregations:
            raise EAException("metric_agg_type must be one of %s" % (str(self.allowed_aggregations)))

        self.rules['aggregation_query_element'] = self.generate_aggregation_query()
        self.ref_window_filled_once = False

        # spike setup
        self.timeframe = self.rules['timeframe']

        self.get_ts = new_get_event_ts(self.ts_field)

        self.ref_windows = {}
        self.cur_windows = {}
        self.first_event = {}
        self.skip_checks = {}


    # required by baseclass
    def generate_aggregation_query(self):
        return {self.metric_key: {self.rules['metric_agg_type']: {'field': self.rules['metric_agg_key']}}}

    # required by baseclass, called by add_aggregation_data
    def check_matches(self, timestamp, query_key, aggregation_data):
        elastalert_logger.info(str(timestamp))
        elastalert_logger.info(str(aggregation_data))

        aggregation_data[self.ts_field] = timestamp
        
        self.handle_event(aggregation_data, aggregation_data[self.metric_key]['value'])

    # spike methods
    def clear_windows(self, qk, event):
        # Reset the state and prevent alerts until windows filled again
        self.skip_checks[qk] = event[self.ts_field] + self.rules['timeframe'] * 2

        self.cur_windows[qk].clear()
        self.ref_windows[qk].clear()

        self.first_event.pop(qk)

    def add_match(self, match, qk):

        reference_value = self.ref_windows[qk].count() / len(self.ref_windows[qk].data)
        spike_value = self.cur_windows[qk].count() / len(self.cur_windows[qk].data)

        extra_info = {'spike_value': spike_value,
                      'reference_value': reference_value}

        match = dict(match.items() + extra_info.items())

        super(SpikeAggregationRule, self).add_match(match)

    def event_matches(self, ref, cur):
        """ Determines if an event spike or dip happening. """

        # Apply threshold limits
        if (cur < self.rules.get('threshold_cur', 0) or
                ref < self.rules.get('threshold_ref', 0)):
            return False

        spike_up, spike_down = False, False
        if cur <= ref / self.rules['spike_height']:
            spike_down = True
        if cur >= ref * self.rules['spike_height']:
            spike_up = True

        if (self.rules['spike_type'] in ['both', 'up'] and spike_up) or \
           (self.rules['spike_type'] in ['both', 'down'] and spike_down):
            return True
        return False

    # shared

    def get_match_str(self, match):
        message = 'An abnormal value of %d occurred around %s for %s:%s.\n' % (
            match['spike_value'],
            pretty_ts(match[self.rules['timestamp_field']], self.rules.get('use_local_time')),
            self.rules['metric_agg_type'],
            self.rules['metric_agg_key'],
        )
        message += 'Preceding that time, there were only %d events within %s\n\n' % (match['reference_value'], self.rules['timeframe'])

        return message

    def handle_event(self, event, value, qk='all'):
        self.first_event.setdefault(qk, event)

        self.ref_windows.setdefault(qk, EventWindow(self.timeframe, None,                        getTimestamp=self.get_ts))
        self.cur_windows.setdefault(qk, EventWindow(self.timeframe, self.ref_windows[qk].append, getTimestamp=self.get_ts))

        self.cur_windows[qk].append((event, value))

        # Don't alert if ref window has not yet been filled for this key AND
        if event[self.ts_field] - self.first_event[qk][self.ts_field] < self.rules['timeframe'] * 2:
            # ElastAlert has not been running long enough for any alerts OR
            if not self.ref_window_filled_once:
                elastalert_logger.info('SpikeAggregationRule.handle_event reference window not filled')
                return
            # This rule is not using alert_on_new_data (with query_key) OR
            if not (self.rules.get('query_key') and self.rules.get('alert_on_new_data')):
                elastalert_logger.info('SpikeAggregationRule.handle_event not alerting on new data')
                return
            # An alert for this qk has recently fired
            if qk in self.skip_checks and event[self.ts_field] < self.skip_checks[qk]:
                elastalert_logger.info('SpikeAggregationRule.handle_event recent alert')
                return
        else:
            self.ref_window_filled_once = True

        # averages values of reference window, `count()` is a running total, a bit misnamed
        reference = self.ref_windows[qk].count() / len(self.ref_windows[qk].data)
        current = self.cur_windows[qk].count() / len(self.cur_windows[qk].data)

        if self.event_matches(reference, current):
            # skip over placeholder events which have count=0
            for match, value in self.cur_windows[qk].data:
                if value:
                    break

            self.add_match(match, qk)
            # self.clear_windows(qk, match)
