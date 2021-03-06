import gc
import unittest
from itertools import product
from itertools import groupby

from flexmock import flexmock
from should_dsl import should

from queryset_iterator import queryset_iterator


VERBOSITY = 2

DEFAULT_QUERYSET_ARGS = {
    'batchsize': 10,
    'gc_collect': True
}

VALID_TEST_DATA = (
    [{'pk': pk, 'pk_info': str(pk)} for pk in range(1, 1000)],
    [{'pk': pk, 'pk_info': str(pk)} for pk in range(100, 200)],
    [{'pk': pk, 'pk_info': str(pk)} for pk in range(10000, 20000, 50)],
    [{'pk': pk, 'pk_info': str(pk)} for pk in range(1, 1000, 2)],
)

VALID_BATCH_SIZES = (5, 10, 20, 40, 80)

VALID_RESULT_SET = VALID_TEST_DATA[0]

LT_EQ_ZERO_TEST_DATA = (0, -1, -2, -3, -4, -5, -100, -9999)

BAD_TYPE_TEST_DATA = ([1], {'2': 3}, 'four', object(), {1, 2}, (3, 4), 1.5)


def create_values_list_mock(result_set):
    result_set_pks = [item['pk'] for item in result_set]
    distinct_mock = flexmock()
    iterator_mock = flexmock()
    (distinct_mock.should_receive('distinct')
     .and_return(iterator_mock)
     .at_most().once())
    (iterator_mock.should_receive('iterator')
     .and_yield(*result_set_pks)
     .at_most().once())
    return distinct_mock


def create_queryset_mock(result_set):
    queryset = flexmock(filter=CountableFilterMock(result_set))
    (queryset.should_receive('values_list')
             .with_args('pk', flat=True)
             .and_return(create_values_list_mock(result_set))
             .at_most().once())
    return queryset


class CountableFilterMock(object):
    def __init__(self, result_set):
        self._result_set = result_set
        self._calls = 0
        self._last_args = ()
        self._last_kwargs = {}

    def __call__(self, *args, **kwargs):
        self._calls += 1
        self._last_args = args
        self._last_kwargs = kwargs.copy()
        args |should| have(0).elements
        kwargs |should| have(1).item
        kwargs.keys()[0] |should| equal_to('pk__in')
        pk_results = [item for item in self.result_set
                      if item['pk'] in kwargs['pk__in']]
        iterator_mock = flexmock()
        (iterator_mock.should_receive('iterator')
                      .and_yield(*pk_results)
                      .at_most().once())
        return iterator_mock

    @property
    def calls(self):
        return self._calls

    @property
    def last_args(self):
        return self._last_args

    @property
    def last_kwargs(self):
        return self._last_kwargs

    @property
    def result_set(self):
        return self._result_set


class Counter(object):
    def __init__(self, start=0):
        self._count = start

    def increment(self):
        self._count += 1

    def reset(self):
        self._count = 0

    @property
    def count(self):
        return self._count


class QuerysetIteratorTestCase(unittest.TestCase):
    def test_fails_on_batch_size_lt_eq_to_zero(self):
        for leq_number in LT_EQ_ZERO_TEST_DATA:
            queryset_mock = create_queryset_mock(VALID_RESULT_SET)
            generator = queryset_iterator(queryset_mock, batchsize=leq_number)
            (list, generator) |should| throw(ValueError)

    def test_fails_on_bad_type_for_batch_size(self):
        for bad_type_arg in BAD_TYPE_TEST_DATA:
            queryset_mock = create_queryset_mock(VALID_RESULT_SET)
            generator = queryset_iterator(queryset_mock, batchsize=bad_type_arg)
            (list, generator) |should| throw(TypeError)

    def test_return_values_correct(self):
        for valid_data in VALID_TEST_DATA:
            queryset_mock = create_queryset_mock(valid_data)
            generator = queryset_iterator(queryset_mock)
            for raw_value, queryset_value in zip(valid_data, generator):
                queryset_value |should| equal_to(raw_value)

    def test_retrieves_in_batches(self):
        for result_set, batchsize in product(VALID_TEST_DATA, VALID_BATCH_SIZES):
            queryset_mock = create_queryset_mock(result_set)
            generator = queryset_iterator(queryset_mock, batchsize=batchsize)
            for batch_number, batch in groupby(enumerate(result_set),
                                               lambda x: x[0] / batchsize):
                keys = [item[1]['pk'] for item in batch]
                try:
                    for _ in xrange(batchsize):
                        generator.next()
                except StopIteration:
                    break
                finally:
                    queryset_mock.filter.calls |should| equal_to(batch_number + 1)
                    (queryset_mock.filter.last_kwargs['pk__in']
                        |should| equal_to(keys))

    def test_gc_collect_at_end_of_batch(self):
        counter = Counter()
        flexmock(gc, collect=counter.increment)
        for result_set, batchsize in product(VALID_TEST_DATA, VALID_BATCH_SIZES):
            counter.reset()
            queryset_mock = create_queryset_mock(result_set)
            generator = queryset_iterator(queryset_mock, batchsize=batchsize)
            i = 0

            while True:
                try:
                    generator.next()
                    i += 1
                except StopIteration:
                    call_count = i / batchsize
                    if i % batchsize:
                        call_count += 1

                    counter.count |should| equal_to(call_count)
                    break
                else:
                    counter.count |should| equal_to((i - 1) / batchsize)


if __name__ == '__main__':
    import nose
    nose.main()
