#!/usr/bin/env python
"""
Python code test - CARTO
"""

import re
from multiprocessing import Pool
from functools import reduce
import urllib.request, urllib.parse
import asyncio
import argparse

__author__ = "Jose Gargallo"
__email__ = "jgargallo@gmail.com"

URL = 'https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv'
MAX_LINE_SIZE = 1024 * 5

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--split', type=int, default=10,
                        help='number of downloads in parallel')
    return parser.parse_args()

def _get_ranges(num_ranges, url=URL):
    """
    Gets Content-Length header to compute the ranges in bytes to be downloaded
    concurrently

    :param num_ranges: number of ranges will be downloaded async
    :param url: url
    :return: list of ranges
    """

    response =  urllib.request.urlopen(url)
    size = int(response.getheader('Content-Length'))
    chunk_size = size // num_ranges
    return [(i, i + chunk_size) for i in range(0, size, chunk_size + 1)]

# this regex is faster than reading the whole line and using split
TIP_AMOUNT_PATTERN = re.compile(r'([^,]+,){15}([^,]+),.+\n')
def _reduce_chunk(chunk):
    tip_amounts = TIP_AMOUNT_PATTERN.findall(chunk.decode('latin1'))
    return (len(tip_amounts), reduce(lambda x, y: x+y, map(lambda x: float(x[1]), tip_amounts)))

def parse_url():
    url = urllib.parse.urlsplit(URL)
    ssl = url.scheme == 'https'
    host, *port = url.netloc.split(':')
    return {
        'host': host,
        'port': int(port[0] if port else (443 if ssl else 80)),
        'ssl': ssl,
        'path': url.path,
    }

async def _fetch_and_reduce_range(pool, start, end):
    url = parse_url()
    reader, writer = await asyncio.open_connection(url['host'], url['port'], ssl=url['ssl'])

    # hack to make sure we don't skip uncompleted lines
    # would fail if uncompleted line is longer than MAX_LINE_SIZE
    # focused on performance, not robustness
    extra_end = end + MAX_LINE_SIZE

    writer.write(('GET {} HTTP/1.1\r\n'.format(url['path']) +
                  'Host: {}\r\n'.format(url['host']) +
                  'Range: bytes={}-{}\r\n'.format(start - 1 if start else 0, extra_end) +
                  '\r\n').encode('latin1'))

    while True:
        line = await reader.readline()
        if line == b'\r\n':
            break

    # first line skipped since is overlapped with previous range
    first_line = await reader.readline()

    # we could do streaming line by line without using multiple processes but wanted
    # to test this approach. Consumes more memory (constant though)
    data = bytearray()
    while True:
        chunk = await reader.read()
        if not chunk:
            break
        data += chunk

    # appends from `end` position to end of first line found to make sure last
    # line on the chunk is completed
    last_pos = end - start - len(first_line)
    if len(data) > last_pos and data[last_pos - 1] != b'\n':
        data = data[:(data.index(b'\n', last_pos) + 1)]

    writer.close()

    # computation takes place in parallel processes to avoid GIL
    # this approach was taken thinking on more time consuming processes than
    # just computing avg
    return pool.apply_async(_reduce_chunk, [data])

async def reduce_tip_amount_avg(ranges, pool):
    reducers = [asyncio.ensure_future(_fetch_and_reduce_range(pool, r[0], r[1])) for r in ranges]
    results = await asyncio.gather(*reducers)

    lines = total = 0
    for r in results:
        r.wait()
        sub_lines, sub_total = r.get()
        lines, total = lines + sub_lines, total + sub_total

    print ('{} lines, {} avg'.format(lines, total / lines))

if __name__ == '__main__':
    args = parse_args()

    ranges = _get_ranges(args.split)

    # creates cpu_count processes
    pool = Pool()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(reduce_tip_amount_avg(ranges, pool))
    loop.close()