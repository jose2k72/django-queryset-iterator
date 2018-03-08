#!/usr/bin/env python
# coding=utf-8
"""
Contains the queryset_iterator function.
This function is useful for iterating over large querysets with Django.
"""
import gc


GC_COLLECT_BATCH = 1

GC_COLLECT_END = 2


def queryset_iterator(queryset, batchsize:int=500, gc_collect:int=GC_COLLECT_BATCH, iterator_return:bool=True):
    """Iterate over a Django queryset in efficient batches

    :param queryset: The queryset to iterate over in batches.
    :type queryset: QuerySet
    :param batchsize: The batch size used to process the queryset. Defaults to 500.
    :type batchsize: int
    :param gc_collect: Whether to garbage collect between batches, at end or not at all.
        Defaults to GC_COLLECT_BATCH.
    :type gc_collect: int
    :param iterator_return: Determines whether to return the iterator or the queryset in such a way that the iteration
        operation is not done within the function. It is a modification to the behavior of the original function.
    :type iterator_return: bool
    :yield: Items within the queryset, one at a time, transparently from batches.
    """
    if batchsize < 1:
        raise ValueError('Batch size must be above 0')

    # if not isinstance(batchsize, int):
    #     raise TypeError('batchsize must be an integer')

    # Acquire a distinct iterator of the primary keys within the queryset.
    # This will be maintained in memory (or a temporary table) within the
    # database and iterated over, i.e. we will not copy and store results.
    iterator = queryset.values_list('pk', flat=True).distinct().iterator()

    # Begin main logic loop. Will loop until iterator is exhausted.
    while True:
        pk_buffer = []
        try:
            # Consume queryset iterator until batch is reached or the
            # iterator has been exhausted.
            while len(pk_buffer) < batchsize:
                pk_buffer.append(iterator.next())
        except StopIteration:
            # Break out of the loop once the queryset has been consumed.
            break
        finally:
            if iterator_return:
                # Use the original queryset to obtain the proper results.
                # Once again using an iterator to keep memory footprint low.
                # This is the behavior of the original function.
                for result in queryset.filter(pk__in=pk_buffer).iterator():
                    yield result
            else:
                # Return the queryset so that the iteration operation is not done within the function.
                # It is a modification to the behavior of the original function.
                yield queryset.filter(pk__in=pk_buffer)

            if gc_collect == GC_COLLECT_BATCH and pk_buffer:
                # Perform a garbage collection to reduce the memory used.
                # Iterating over large datasets can be quite costly on memory.
                gc.collect()

    if gc_collect == GC_COLLECT_END:
        # Perform a garbage collection to reduce the memory used.
        gc.collect()


def queryset_iterator_qs(queryset, batchsize=500, gc_collect=GC_COLLECT_BATCH):
    """Iterate over a Django queryset in efficient batches
    Returns the queryset so that the iteration operation is not done within the function.
    It is a modification of the original function.

    :param queryset: The queryset to iterate over in batches.
    :type queryset: QuerySet
    :param batchsize: The batch size used to process the queryset. Defaults to 500.
    :type batchsize: int
    :param gc_collect: Whether to garbage collect between batches, at end or not at all.
        Defaults to GC_COLLECT_BATCH.
    :type gc_collect: int
    :yield: Batch of items within the queryset, in groups of size [batchsize]
    """
    if batchsize < 1:
        raise ValueError('Batch size must be above 0')

    if not isinstance(batchsize, int):
        raise TypeError('batchsize must be an integer')

    # Acquire a distinct iterator of the primary keys within the queryset.
    # This will be maintained in memory (or a temporary table) within the
    # database and iterated over, i.e. we will not copy and store results.
    iterator = queryset.values_list('pk', flat=True).distinct().iterator()

    # Begin main logic loop. Will loop until iterator is exhausted.
    while True:
        pk_buffer = []
        try:
            # Consume queryset iterator until batch is reached or the
            # iterator has been exhausted.
            while len(pk_buffer) < batchsize:
                pk_buffer.append(iterator.__next__())
        except StopIteration:
            # Break out of the loop once the queryset has been consumed.
            break
        finally:
            # Use the original queryset to obtain the proper results.
            # Once again using an iterator to keep memory footprint low.
            yield queryset.filter(pk__in=pk_buffer)

            if gc_collect == GC_COLLECT_BATCH and pk_buffer:
                # Perform a garbage collection to reduce the memory used.
                # Iterating over large datasets can be quite costly on memory.
                gc.collect()

    if gc_collect == GC_COLLECT_END:
        # Perform a garbage collection to reduce the memory used.
        gc.collect()
