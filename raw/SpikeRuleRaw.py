class SpikeRule(RuleType):
    """ A rule that uses two sliding windows to compare relative event frequency. """
    required_options = frozenset(['timeframe', 'spike_height', 'spike_type'])

    def __init__(self, *args):
        super(SpikeRule, self).__init__(*args)
        self.timeframe = self.rules['timeframe']

        self.ref_windows = {}
        self.cur_windows = {}

        self.ts_field = self.rules.get('timestamp_field', '@timestamp')
        self.get_ts = new_get_event_ts(self.ts_field)
        self.first_event = {}
        self.skip_checks = {}

        self.ref_window_filled_once = False

    def add_count_data(self, data):
        """ Add count data to the rule. Data should be of the form {ts: count}. """
        if len(data) > 1:
            raise EAException('add_count_data can only accept one count at a time')
        for ts, count in data.iteritems():
            self.handle_event({self.ts_field: ts}, count, 'all')

    def add_terms_data(self, terms):
        for timestamp, buckets in terms.iteritems():
            for bucket in buckets:
                count = bucket['doc_count']
                event = {self.ts_field: timestamp,
                         self.rules['query_key']: bucket['key']}
                key = bucket['key']
                self.handle_event(event, count, key)

    def add_data(self, data):
        for event in data:
            qk = self.rules.get('query_key', 'all')
            if qk != 'all':
                qk = hashable(lookup_es_key(event, qk))
                if qk is None:
                    qk = 'other'
            self.handle_event(event, 1, qk)

    def clear_windows(self, qk, event):
        # Reset the state and prevent alerts until windows filled again
        self.cur_windows[qk].clear()
        self.ref_windows[qk].clear()
        self.first_event.pop(qk)
        self.skip_checks[qk] = event[self.ts_field] + self.rules['timeframe'] * 2

    def handle_event(self, event, count, qk='all'):
        self.first_event.setdefault(qk, event)

        self.ref_windows.setdefault(qk, EventWindow(self.timeframe, getTimestamp=self.get_ts))
        self.cur_windows.setdefault(qk, EventWindow(self.timeframe, self.ref_windows[qk].append, self.get_ts))

        self.cur_windows[qk].append((event, count))

        # Don't alert if ref window has not yet been filled for this key AND
        if event[self.ts_field] - self.first_event[qk][self.ts_field] < self.rules['timeframe'] * 2:
            # ElastAlert has not been running long enough for any alerts OR
            if not self.ref_window_filled_once:
                return
            # This rule is not using alert_on_new_data (with query_key) OR
            if not (self.rules.get('query_key') and self.rules.get('alert_on_new_data')):
                return
            # An alert for this qk has recently fired
            if qk in self.skip_checks and event[self.ts_field] < self.skip_checks[qk]:
                return
        else:
            self.ref_window_filled_once = True

        if self.find_matches(self.ref_windows[qk].count(), self.cur_windows[qk].count()):
            # skip over placeholder events which have count=0
            for match, count in self.cur_windows[qk].data:
                if count:
                    break

            self.add_match(match, qk)
            self.clear_windows(qk, match)

    def add_match(self, match, qk):
        extra_info = {}
        spike_count = self.cur_windows[qk].count()
        reference_count = self.ref_windows[qk].count()
        extra_info = {'spike_count': spike_count,
                      'reference_count': reference_count}

        match = dict(match.items() + extra_info.items())

        super(SpikeRule, self).add_match(match)

    def find_matches(self, ref, cur):
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

    def get_match_str(self, match):
        message = 'An abnormal number (%d) of events occurred around %s.\n' % (
            match['spike_count'],
            pretty_ts(match[self.rules['timestamp_field']], self.rules.get('use_local_time'))
        )
        message += 'Preceding that time, there were only %d events within %s\n\n' % (match['reference_count'], self.rules['timeframe'])
        return message

    def garbage_collect(self, ts):
        # Windows are sized according to their newest event
        # This is a placeholder to accurately size windows in the absence of events
        for qk in self.cur_windows.keys():
            # If we havn't seen this key in a long time, forget it
            if qk != 'all' and self.ref_windows[qk].count() == 0 and self.cur_windows[qk].count() == 0:
                self.cur_windows.pop(qk)
                self.ref_windows.pop(qk)
                continue
            placeholder = {self.ts_field: ts}
            # The placeholder may trigger an alert, in which case, qk will be expected
            if qk != 'all':
                placeholder.update({self.rules['query_key']: qk})
            self.handle_event(placeholder, 0, qk)
