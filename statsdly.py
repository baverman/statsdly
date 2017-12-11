import logging
import argparse
import socket

from time import time, sleep
from threading import Lock, Thread
from collections import Counter
from bisect import insort
from math import floor, ceil
from statistics import mean, pstdev
from urllib.parse import urlsplit

version = '0.3'
PREFIX = b''
FLUSH_INTERVAL = 60
HOST = '127.0.0.1'
PORT = 8125
RECYCLE = 300

GRAPHITE_HOST = None
GRAPHITE_PORT = 2003

PERCENTILES = (50, 75, 95, 99)
DELTA_CHARS = b'-'[0], b'+'[0]

log = logging.getLogger('statsdly')


def percentile(N, percent):
    if not N:  # pragma: no cover
        return None
    k = (len(N) - 1) * percent / 100.0
    f = floor(k)
    c = ceil(k)
    if f == c:
        return N[int(k)]
    d0 = N[int(f)] * (c-k)
    d1 = N[int(c)] * (k-f)
    return d0 + d1


class State:
    def __init__(self, gauges=None):
        self.counters = Counter()
        self.timers = {}
        self.timers_count = Counter()
        self.gauges = {} if gauges is None else gauges
        self.sets = {}

    def handle_counter(self, name, value, rate):
        self.counters[name] += value / rate

    def handle_timer(self, name, value, rate):
        insort(self.timers.setdefault(name, []), value / 1000)
        self.timers_count[name] += 1 / rate

    def handle_gauge(self, name, value, rate):
        value, is_delta = value
        if is_delta:
            try:
                self.gauges[name] += value / rate
            except KeyError:
                pass
        else:
            self.gauges[name] = value

    def handle_set(self, name, value, _rate):
        self.sets.setdefault(name, set()).add(value)

    def extract(self):
        for k, v in self.counters.items():
            yield b'%s.count' % k, v

        for k, v in self.timers_count.items():
            yield b'%s.count' % k, v

        for k, v in self.timers.items():
            m = mean(v)
            yield b'%s.upper' % k, v[-1]
            yield b'%s.lower' % k, v[0]
            yield b'%s.mean' % k, m
            yield b'%s.stdev' % k, pstdev(v, m)
            for p in PERCENTILES:
                yield b'%s.p%d' % (k, p), percentile(v, p)

        for k, v in self.gauges.items():
            yield k, v

        for k, v in self.sets.items():
            yield k, len(v)

    def to_graphite(self, ts):
        ts = b'%d' % int(ts)
        return b''.join(b'%s%s %g %s\n' % (PREFIX, name, value, ts)
                        for name, value in self.extract())


TYPES = {
    b'c': State.handle_counter,
    b'ms': State.handle_timer,
    b'g': State.handle_gauge,
    b's': State.handle_set
}


flush_lock = Lock()
state = State()


def timer(interval):  # pragma: no cover
    next_tick = time()
    while True:
        next_tick += interval
        to_sleep = next_tick - time()
        if to_sleep > 0:
            sleep(to_sleep)
            yield next_tick


def swap(oldstate):
    return State(oldstate.gauges.copy()), oldstate


def get_connection(host, port):  # pragma: no cover
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 128)
    s.settimeout(5)
    wait = 1
    backoff = 1.5
    max_time = 60
    while True:
        try:
            s.connect((host, port))
            s.settimeout(0)
            log.info('Connected to graphite at %s:%d', host, port)
            return s
        except Exception as e:
            log.error('Error connecting to graphite at %s:%d (%s), wait %ds', host, port, e, wait)

        sleep(wait)
        wait = min(wait * backoff, max_time)


def flusher():  # pragma: no cover
    global state
    cn = None
    for ts in timer(FLUSH_INTERVAL):
        with flush_lock:
            state, oldstate = swap(state)

        msg = payload = oldstate.to_graphite(ts)
        if not msg:
            continue

        log.debug('Start flush for %s', ts)
        while msg:
            if cn and ts - last_cn > RECYCLE:
                cn = None

            if not cn:
                cn = get_connection(GRAPHITE_HOST, GRAPHITE_PORT)
                last_cn = ts

            try:
                sent = cn.send(msg[:4096])
                msg = msg[sent:]
            except Exception:
                log.error('Error sending data for %s', ts)
                cn = None
        log.debug('End flush for %s, payload: %r', ts, payload)


def parse_value(line):
    name, sep, value = line.partition(b':')
    if not sep:
        return None

    parts = value.split(b'|')
    lparts = len(parts)
    if lparts == 2:
        rate = 1
    elif lparts == 3:
        try:
            rate = float(parts[2][1:])
        except ValueError:
            return None
    else:
        return None

    typ = parts[1]
    if typ != b's':
        try:
            val = float(parts[0])
        except ValueError:
            return None
    else:
        val = parts[0]

    if typ == b'g':
        val = (val, parts[0][0] in DELTA_CHARS)

    return name, typ, val, rate


def handle_data(data, state):
    for line in data.splitlines():
        metric = parse_value(line)
        if not metric:
            continue
        name, t, value, rate = metric
        handler = TYPES.get(t)
        handler and handler(state, name, value, rate)


def loop():  # pragma: no cover
    flush_thread = Thread(target=flusher)
    flush_thread.daemon = True
    flush_thread.start()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind((HOST, PORT))
        while True:
            data, _addr = s.recvfrom(4096)
            if data:
                with flush_lock:
                    handle_data(data, state)


def host_port(default_port, default_host=None):
    def inner(text):
        if text.startswith(':'):
            if not default_host:
                raise ValueError('Host required')
            text = default_host + text
        url = urlsplit('tmp://' + text)
        return (url.hostname, url.port or default_port)
    return inner


def csvint(text):
    return list(map(int, filter(None, text.split(','))))


def main():  # pragma: no cover
    global FLUSH_INTERVAL, HOST, PORT, GRAPHITE_HOST, GRAPHITE_PORT, \
           PERCENTILES, PERCENTILES, RECYCLE, PREFIX
    parser = argparse.ArgumentParser(description='StatsD server')

    parser.add_argument('-v', action='count', default=0,
                        help='increase verbosity', dest='verbosity')

    parser.add_argument('-l',dest='listen', default=f'{HOST}:{PORT}',
                        help='listen on host:port, default is %(default)s',
                        metavar='host[:port]', type=host_port(HOST, PORT))

    parser.add_argument('-f', dest='flush_interval', type=int, default=FLUSH_INTERVAL,
                        help='flush interval, default is %(default)d seconds',
                        metavar='seconds')

    parser.add_argument('--percentiles', dest='percentiles', default=','.join(map(str, PERCENTILES)),
                        help=f'timer percentiles as csv, default is %(default)r',
                        metavar='p1,p2,...', type=csvint)

    parser.add_argument('-p', '--prefix', dest='prefix',
                        help=f'prefix to all metrics', metavar='prefix')

    parser.add_argument('--recycle', dest='recycle', default=RECYCLE, type=int,
                        help=f'reconnect to carbon after this amount of seconds, default is %(default)ds',
                        metavar='seconds')

    parser.add_argument('-g', dest='graphite', required=True,
                        help=f'graphite host:port for sending metrics, default port is {GRAPHITE_PORT}',
                        metavar='host[:port]', type=host_port(2003))

    args = parser.parse_args()

    FLUSH_INTERVAL = args.flush_interval
    HOST, PORT = args.listen
    GRAPHITE_HOST, GRAPHITE_PORT = args.graphite
    PERCENTILES = args.percentiles
    PREFIX = ((args.prefix or PREFIX.decode()).rstrip('.') + '.').encode()
    RECYCLE = args.recycle

    if args.verbosity > 1:
        level = 'DEBUG'
    elif args.verbosity > 0:
        level = 'INFO'
    else:
        level = 'WARN'
    logging.basicConfig(level=level, format='[%(asctime)s] %(name)s:%(levelname)s %(message)s')

    try:
        loop()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':  # pragma: no cover
    main()
