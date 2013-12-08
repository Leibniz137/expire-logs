#!/usr/bin/env python

"""
If daily indices (i.e. --days-to-keep), deletes all indices
    with a datestamp older than "--days-to-keep".
If hourly indices, it will delete all of those older than
    "--hours-to-keep".

This script presumes an index is named typically,
e.g. logstash-YYYY.MM.DD. It will work with any name-YYYY.MM.DD
or name-YYYY.MM.DD.HH type sequence

Requires python and the following dependencies:
    - pyes (python elasticsearch bindings, which might need simplejson)
    - argparse (if python --version < 2.7)

TODO: Proper logging instead of just print statements, being able to
configure a decent logging level.
      - Unit tests. The code is somewhat broken up into logical
            parts that may be tested separately.
      - Better error reporting?
      - Improve the get_index_epoch method to parse more date formats.
        Consider renaming (to "parse_date_to_timestamp"?)
"""

import sys
import time
import argparse
from datetime import timedelta

import pyes


__version__ = '0.1.2'


def parser():
    """ Creates an ArgumentParser to parse the command line options. """
    parser = argparse.ArgumentParser(description="Delete old logstash indices "
                                                 "from Elasticsearch.")
    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s '+__version__)
    parser.add_argument('--host', default='localhost',
                        help='Elasticsearch host.')
    parser.add_argument('--port', type=int, default=9200,
                        help='Elasticsearch port')
    parser.add_argument('-t', '--timeout', type=int, default=30,
                        help='Elasticsearch timeout')
    parser.add_argument('-p', '--prefix', default='logstash-',
                        help="Prefix for the indices. "
                        "Indices that do not have this prefix are skipped.")
    parser.add_argument('-s', '--separator', default='.',
                        help='Time unit separator')
    parser.add_argument('-H', '--hours-to-keep', action='store', type=int,
                        help='Number of hours to keep.')
    parser.add_argument('-d', '--days-to-keep', action='store', type=int,
                        help='Number of days to keep.')
    parser.add_argument('-g', '--disk-space-to-keep', action='store',
                        type=float, help='Disk space to keep (GB).')
    parser.add_argument('-n', '--dry-run', action='store_true', default=False,
                        help="If true, does not perform any changes to "
                        "the Elasticsearch indices.")
    return parser


def get_index_epoch(index_timestamp, separator='.'):
    """ Gets the epoch of the index.

    :param index_timestamp: A string on the format YYYY.MM.DD[.HH]
    :return The creation time (epoch) of the index.
    """
    year_month_day_optionalhour = index_timestamp.split(separator)
    if len(year_month_day_optionalhour) == 3:
        year_month_day_optionalhour.append('3')

    return time.mktime([int(part) for part in year_month_day_optionalhour] +
                       [0, 0, 0, 0, 0])


def find_expired_indices(connection, days_to_keep=None, hours_to_keep=None,
                         separator='.', prefix='logstash-', out=sys.stdout,
                         err=sys.stderr):
    """ Generator that yields expired indices.

    :return: Yields tuples on the format ``(index_name, expired_by)``
        where index_name is the name of the expired index and
        expired_by is the number of seconds (a float value) that
        the index was expired by.
    """
    utc_now_time = time.time() + time.altzone
    days_cutoff = (utc_now_time - days_to_keep * 24 * 60 * 60
                   if days_to_keep is not None else None)
    hours_cutoff = (utc_now_time - hours_to_keep * 60 * 60
                    if hours_to_keep is not None else None)

    for index_name in sorted(set(connection.get_indices().keys())):
        if not index_name.startswith(prefix):
            print('Skipping index due to missing prefix {0}: {1}'
                  .format(prefix, index_name))
            continue
        unprefixed_index_name = index_name[len(prefix)+1:]

        # find the timestamp parts (i.e ['2011', '01', '05'] from '2011.01.05')
        # using the configured separator
        parts = unprefixed_index_name.split(separator)

        # perform some basic validation
        if (len(parts) < 3 or len(parts) > 4 or
                not all([item.isdigit() for item in parts])):
            print('Could not find a valid timestamp from the index: '
                  .format(index_name))
            continue

        # find the cutoff. if we have more than 3 parts in the timestamp,
        # the timestamp includes the hours and we should compare it to
        # the hours_cutoff, otherwise, we should use the days_cutoff.
        cutoff = hours_cutoff
        if len(parts) == 3:
            cutoff = days_cutoff

        # but the cutoff might be none, if the current index only has three
        # parts (year.month.day) and we're only removing hourly indices:
        if cutoff is None:
            print("Skipping {0} because it is of a type (hourly or daily) "
                  "that I'm not asked to delete.".format(index_name))
            continue

        index_epoch = get_index_epoch(unprefixed_index_name)

        # if the index is older than the cutoff
        if index_epoch < cutoff:
            yield index_name, cutoff-index_epoch

        else:
            print("{0} is {1} above the cutoff."
                  .format(index_name, timedelta(seconds=index_epoch-cutoff)))


def find_overusage_indices(connection, disk_space_to_keep, separator='.',
                           prefix='logstash-', out=sys.stdout, err=sys.stderr):
    """ Generator that yields over usage indices.

    :return: Yields tuples on the format ``(index_name, 0)`` where index_name
    is the name of the expired index. The second element is only here for
    compatiblity reasons.
    """

    disk_usage = 0.0
    disk_limit = disk_space_to_keep * 2**30

    for index_name in reversed(sorted(set(connection.get_indices().keys()))):
        if not index_name.startswith(prefix):
            print('Skipping index due to missing prefix {0}: {1}'
                  .format(prefix, index_name))
            continue

        index_size = (
            connection.status(index_name)
            .get('indices')
            .get(index_name)
            .get('index')
            .get('primary_size_in_bytes')
        )
        disk_usage += index_size

        if disk_usage > disk_limit:
            yield index_name, 0
        else:
            print("keeping {0}, disk usage is {1:.3f} GB and "
                  "disk limit is {2:.3f} GB."
                  .format(index_name, disk_usage/2**30, disk_limit/2**30))


def main():
    start = time.time()
    arguments = parser().parse_args()

    if (not arguments.hours_to_keep and
            not arguments.days_to_keep and
            not arguments.disk_space_to_keep):
        print('Invalid arguments: You must specify either the number of hours,'
              ' the number of days to keep or the maximum disk space to use')
        parser.print_help()
        return

    address = '{0}:{1}'.format(arguments.host, arguments.port)
    connection = pyes.ES(address, timeout=arguments.timeout)

    if arguments.days_to_keep:
        print('Deleting daily indices older than {0} days.'
              .format(arguments.days_to_keep))
        expired_indices = find_expired_indices(connection,
                                               arguments.days_to_keep,
                                               arguments.hours_to_keep,
                                               arguments.separator,
                                               arguments.prefix)
    if arguments.hours_to_keep:
        print('Deleting hourly indices older than {0} hours.'
              .format(arguments.hours_to_keep))
        expired_indices = find_expired_indices(connection,
                                               arguments.days_to_keep,
                                               arguments.hours_to_keep,
                                               arguments.separator,
                                               arguments.prefix)
    if arguments.disk_space_to_keep:
        print("Let's keep disk usage lower than {0} GB."
              .format(arguments.disk_space_to_keep))
        expired_indices = find_overusage_indices(connection,
                                                 arguments.disk_space_to_keep,
                                                 arguments.separator,
                                                 arguments.prefix)
    print('')
    for index_name, expired_by in expired_indices:
        expiration = timedelta(seconds=expired_by)
        if arguments.dry_run:
            print("Would have attempted deleting index {0} because it is {1} "
                  "older than the calculated cutoff."
                  .format(index_name, expiration))
            continue

        print("Deleting index {0} because it was {1} older than cutoff."
              .format(index_name, expiration))
        deletion = connection.delete_index_if_exists(index_name)
        # On success ES returns a dict on the form:
        #   {u'acknowledged': True, u'ok': True}
        if deletion.get('ok'):
            print("Successfully deleted index: {0}".format(index_name))
        else:
            print("Error deleting index: {0}. ({1})"
                  .format(index_name, deletion))

    print('\nDone in {0}.'.format(timedelta(seconds=time.time() - start)))


if __name__ == '__main__':
    main()
