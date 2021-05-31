from vnpy.trader.constant import Interval, Exchange

_interval_mapper = {
    "1m": Interval.MINUTE,
    "1h": Interval.HOUR,
    "d": Interval.DAILY,
    "w": Interval.WEEKLY
}

_exchange_mapper = {
    "COINBASE": Exchange.COINBASE
}


def map_stock_interval_to_internal(interval: str) -> Interval:
    if interval not in _interval_mapper:
        raise Exception()
    return _interval_mapper.get(interval)


def map_stock_exchange_to_internal(exchange: str) -> Exchange:
    if exchange not in _exchange_mapper:
        raise Exception()
    return _exchange_mapper.get(exchange)