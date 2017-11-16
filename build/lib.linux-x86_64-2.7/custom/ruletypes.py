# -*- coding: utf-8 -*-
# https://github.com/Yelp/elastalert/issues/1288
# http://elastalert.readthedocs.io/en/latest/recipes/adding_rules.html
"""
Rule types that count one document multiple times according to a field in the
document. This rule may only be used when the contents of the document may be
discarded (i.e. use_count_query would have sufficed), as it does not
preserve them.
"""
import copy

import threading

from blist import sortedlist

from elastalert.ruletypes import hashable

from elastalert.ruletypes import new_get_event_ts
from elastalert.ruletypes import SpikeRule
from elastalert.ruletypes import EventWindow
from elastalert.util import lookup_es_key
from elastalert.util import elastalert_logger
from elastalert.util import pretty_ts


class PercentileOfFieldSpikeRule(SpikeRule):

    def __init__(self, *args):
        super(PercentileOfFieldSpikeRule, self).__init__(*args)

        self.skip_test = {}

    """
    Metaclass that creates a subclass of the class's backing_rule_type.
    backing_rule_type must implement add_count_data.
    """
    # This allows the add_data to use super(...).add_count_data(...) to call
    # the add_count_data method of the appropriate backing RuleType, without
    # having to reimplement all the common logic for each backing_rule_type.

    required_options = frozenset(['percentile_value', 'target_field'])

    def add_data(self, data):
        for document in data:

            qk = self.rules.get('query_key', 'all')

            if qk != 'all':
                qk = hashable(lookup_es_key(document, qk))
                if qk is None:
                    elastalert_logger.warning("Didn't find Qk in document.")
                    qk = 'other'
            
            # has to @timestamp
            ts = lookup_es_key(document, self.ts_field)
            # has to be integer
            count = lookup_es_key(document, self.rules['target_field'])
            
            if count and ts:
                self.handle_event({self.ts_field: ts}, count, qk)
            else:
                elastalert_logger.warning(
                    'Did not find field %s representing target_field in '
                    'document for rule %s' % ( self.rules['target_field'], self.rules['name']))

    def handle_event(self, event, count, qk='all'):

        self.first_event.setdefault(qk, event)

        self.ref_windows.setdefault(qk, CustomEventWindow(self.timeframe, None,                        self.get_ts,self.rules.get('percentile_value')))
        self.cur_windows.setdefault(qk, CustomEventWindow(self.timeframe, self.ref_windows[qk].append, self.get_ts,self.rules.get('percentile_value')))

        self.cur_windows[qk].append((event, count))

        # Don't alert if ref window has not yet been filled for this key AND
        if event[self.ts_field] - self.first_event[qk][self.ts_field] <= self.rules['timeframe'] * 2:

            # ElastAlert has not been running long enough for any alerts OR
            if not self.ref_window_filled_once:
                return

            # This rule is not using alert_on_new_data (with query_key) OR
            if not (self.rules.get('query_key') and self.rules.get('alert_on_new_data')):
                return

        else:
            self.ref_window_filled_once = True

        # An alert for this qk has recently fired
        if qk in self.skip_test and event[self.ts_field] < self.skip_test[qk]:
            return

        ref_count = self.ref_windows[qk].count()
        cur_count = self.cur_windows[qk].count()

        #if (cur_count == 20):
        #    ref_validator = [[test[0]['ts'].strftime("%m/%d/%Y %H:%M:%S"),test[1]] for test in self.ref_windows[qk].data]
        #    cur_validator = [[test[0]['ts'].strftime("%m/%d/%Y %H:%M:%S"),test[1]] for test in self.cur_windows[qk].data]
        #    pass

        if self.find_matches(ref_count,cur_count):
            # skip over placeholder events which have count=0
            for match, count in self.cur_windows[qk].data:
                if count >= cur_count:
                    break

            self.add_match(match, qk)
            self.clear_windows(qk, match)

    def clear_windows(self, qk, event):
        # Reset the state and prevent alerts until windows filled again
        self.skip_test[qk] =  event[self.ts_field] + self.rules['timeframe'] * 2
        self.cur_windows[qk].clear()
        self.ref_windows[qk].clear()
        self.first_event.pop(qk)

    def add_match(self, match, qk):
        extra_info = {'current_count': self.cur_windows[qk].count(),
                      'reference_count': self.ref_windows[qk].count()}

        elastalert_logger.info('current_count: %s , reference_count: %s'
            %(self.cur_windows[qk].count(),self.ref_windows[qk].count()))

        match = dict(match.items() + extra_info.items())

        super(SpikeRule, self).add_match(match)

    def get_match_str(self, match):
        message = 'An abnormal number (%d) of events occurred around %s.\n' % (
            match['current_count'],
            pretty_ts(match[self.rules['timestamp_field']], self.rules.get('use_local_time'))
        )
        message += 'Preceding that time, there were only %d events within %s\n\n' % (match['reference_count'], self.rules['timeframe'])
        return message


class CustomEventWindow(EventWindow):
    """ A container for hold event counts for rules which need a chronological ordered event window. """

    def __init__(self, timeframe, onRemoved=None, getTimestamp=new_get_event_ts('@timestamp'), p_value=90):
        super(CustomEventWindow, self ).__init__(timeframe, onRemoved, getTimestamp)         
        self.p_value = p_value

    def append(self, event):
        """ Add an event to the window. Event should be of the form (dict, count).
        This will also pop the oldest events and call onRemoved on them until the
        window size is less than timeframe. """
        self.data.add(event)

        while self.duration() >= self.timeframe:
            oldest = self.data[0]
            self.data.remove(oldest)
            self.onRemoved and self.onRemoved(oldest)

    def append_middle(self, event):
        """ Attempt to place the event in the correct location in our deque.
        Returns True if successful, otherwise False. """
        raise NotImplementedError

    def count(self):

        temp_list = sorted(self.data, key=lambda data: data[1])
        p_posit = int(len(temp_list) * self.p_value/100) - 1
        self.running_count = temp_list[p_posit][1]

        return self.running_count
