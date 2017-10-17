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

from elastalert.ruletypes import SpikeRule
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
                ts = lookup_es_key(document, self.ts_field)
                if count and ts:
                    super(self.__class__, self).add_count_data({ts: count})

        backing_rule_type_cls = dct['backing_rule_type']
        dct['required_options'] = (
            dct['backing_rule_type'].required_options | frozenset(['target_field']))
        dct['add_data'] = add_data
        return type.__new__(mcs, name, (backing_rule_type_cls,), dct)


class SumOfFieldSpikeRule(object):
    __metaclass__ = SumOfFieldRuleFactory
    backing_rule_type = SpikeRule