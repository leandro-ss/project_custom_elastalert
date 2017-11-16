# -*- coding: utf-8 -*-
"""
Rule types that count one document multiple times according to a field in the
document. This rule may only be used when the contents of the document may be
discarded (i.e. use_count_query would have sufficed), as it does not
preserve them.
"""
import copy

from elastalert.ruletypes import RuleType
from elastalert.ruletypes import FrequencyRule
from elastalert.ruletypes import SpikeRule
from elastalert.ruletypes import FlatlineRule
from elastalert.ruletypes import EventWindow
from elastalert.util import lookup_es_key
from elastalert.util import elastalert_logger
from elastalert.util import pretty_ts


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


        backing_rule_type_cls = dct['backing_rule_type']
        dct['required_options'] = (
            dct['backing_rule_type'].required_options | frozenset(['target_field']))
        dct['add_data'] = add_data
        dct['handle_event'] = handle_event
        return type.__new__(mcs, name, (backing_rule_type_cls,), dct)


class SumOfFieldFrequencyRule(object):
    __metaclass__ = SumOfFieldRuleFactory
    backing_rule_type = FrequencyRule


class SumOfFieldSpikeRule(object):
    __metaclass__ = SumOfFieldRuleFactory
    backing_rule_type = SpikeRule