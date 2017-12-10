import pytest
import statsdly
from statsdly import handle_data, State, host_port, csvint

statsdly.PERCENTILES = 50,


def test_counter():
    state = State()
    handle_data(b'boo:1|c\nboo:1|c|@0.1\n', state)
    assert list(state.extract()) == [(b'boo.count', 11)]


def test_timer_odd():
    state = State()
    handle_data(b'boo:1|ms\nboo:2|ms\nboo:3|ms|@0.1\n', state)
    assert list(state.extract()) == [(b'boo.count', 12), (b'boo.upper', 0.003),
                                     (b'boo.lower', 0.001), (b'boo.mean', 0.002),
                                     (b'boo.p50', 0.002)]

def test_timer_even():
    state = State()
    handle_data(b'boo:1|ms\nboo:2|ms\nboo:3|ms\nboo:4|ms\n', state)
    assert list(state.extract()) == [(b'boo.count', 4), (b'boo.upper', 0.004),
                                     (b'boo.lower', 0.001), (b'boo.mean', 0.0025),
                                     (b'boo.p50', 0.0025)]


def test_set():
    state = State()
    handle_data(b'boo:1|s\nboo:1|s\nboo:2|s\n', state)
    assert list(state.extract()) == [(b'boo', 2)]


def test_gauge():
    state = State()
    handle_data(b'boo:1|g\nboo:+2|g\nboo:-1|g|@0.1\n', state)
    assert list(state.extract()) == [(b'boo', -7)]


def test_delta_gauge_with_empty_key():
    state = State()
    handle_data(b'boo:+1|g\n', state)
    assert list(state.extract()) == []


def test_to_graphite():
    state = State()
    handle_data(b'boo:1|c\nboo:1|c|@0.1\n', state)
    assert state.to_graphite(101) == b'boo.count 11 101\n'


def test_bad_data():
    state = State()

    handle_data(b'boo\n', state)
    assert not list(state.extract())

    handle_data(b'boo:1|c|@boo\n', state)
    assert not list(state.extract())

    handle_data(b'boo:1\n', state)
    assert not list(state.extract())

    handle_data(b'boo:1|c|@|b\n', state)
    assert not list(state.extract())

    handle_data(b'boo:foo|c\n', state)
    assert not list(state.extract())


def test_host_port():
    assert host_port(2003)('boo') == ('boo', 2003)
    assert host_port(2003)('boo:42') == ('boo', 42)

    assert host_port(2003, 'boo')(':8080') == ('boo', 8080)
    with pytest.raises(ValueError):
        host_port(2003)(':8080')


def test_csvint():
    assert csvint('10,20,') == [10, 20]
    with pytest.raises(ValueError):
        csvint('20,boo')
