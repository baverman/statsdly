statsdly
========

StatsD server implemenation compatible with python3.
For python2 support see `Bucky <https://github.com/trbs/bucky>`_.

Metrics are flushed into graphite/carbon.


Features
--------

* Support for counters, timers, gauges and sets.
* Gauge values are kept between flushes.
* Counters, timers and sets are emptied on every flash.
* Counter metrics have ``.count`` postfix.
* Timer metrics have ``upper``, ``lower``, ``mean``, ``stdev`` and
  configurable percentile values.
* Clean metric names with prefix (``-p`` command line option) and name sent by
  a client only.
* Low footprint.


Usage
-----

::

    usage: statsdly [-h] [-v] [-l host[:port]] [-f seconds]
                    [--percentiles p1,p2,...] [-p prefix] [--recycle seconds] -g
                    host[:port]

    optional arguments:
      -h, --help            show this help message and exit
      -v                    increase verbosity
      -l host[:port]        listen on host:port, default is 127.0.0.1:8125
      -f seconds            flush interval, default is 60 seconds
      --percentiles p1,p2,...
                            timer percentiles as csv, default is '50,75,95,99'
      -p prefix, --prefix prefix
                            prefix to all metrics
      --recycle seconds     reconnect to carbon after this amount of seconds,
                            default is 300s
      -g host[:port]        graphite host:port for sending metrics, default port
                            is 2003
