import tornado.ioloop
import tornado.web
import threading
import json
import time
from datetime import datetime

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy.gateway.ctp import CtpGateway
from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.coinbase import CoinbaseGateway
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_backtester import CtaBacktesterApp
from vnpy.app.algo_trading import AlgoTradingApp
from vnpy.app.data_recorder import DataRecorderApp
from vnpy.app.portfolio_manager import PortfolioManagerApp
from vnpy.app.portfolio_strategy import PortfolioStrategyApp
from vnpy.app.chart_wizard import ChartWizardApp

from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import HistoryRequest
from server.mapper.mapper import *


class StockDataHandler(tornado.web.RequestHandler):
    def initialize(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine

    def post(self):
        data = json.loads(self.request.body)
        if data is None or \
                "symbol" not in data or \
                "interval" not in data or \
                "exchange" not in data or \
                "start_ts" not in data or \
                "end_ts" not in data:
            self.set_status(500, "invalid request body")
            return
        symbol = data["symbol"]
        interval = map_stock_interval_to_internal(data["interval"])
        exchange = map_stock_exchange_to_internal(data["exchange"])
        start_ts = data["start_ts"]
        end_ts = data["end_ts"]

        start_dt = datetime.utcfromtimestamp(start_ts)
        end_dt = datetime.utcfromtimestamp(end_ts)

        print("sss:{}, ee:{}".format(start_dt, end_dt))

        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start_dt,
            end=end_dt,
        )

        res = self.main_engine.query_history(req, data["exchange"])
        res_arr = []
        for d in res:
            res_arr.append({
                "ts": d.datetime.timestamp(),
                "H": d.high_price,
                "L": d.low_price,
                "O": d.open_price,
                "C": d.close_price,
                "V": d.volume
            })
        res_dic = {
            "res": res_arr
        }

        self.write(res_dic)


def start_vnpy_app(main_engine: MainEngine, event_engine: EventEngine):
    """Start VN Trader"""
    qapp = create_qapp()

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


def start_tornado_app(main_engine: MainEngine, event_engine: EventEngine):
    return tornado.web.Application(
        [
            (r"/stock_data", StockDataHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
        ]
    )


def main():
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(BinanceGateway)
    main_engine.add_gateway(CoinbaseGateway)

    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(DataRecorderApp)
    main_engine.add_app(PortfolioManagerApp)
    main_engine.add_app(PortfolioStrategyApp)
    main_engine.add_app(ChartWizardApp)

    vnpy_thread = threading.Thread(target=start_vnpy_app, args=[main_engine, event_engine], daemon=True)
    vnpy_thread.start()

    tornado_app = start_tornado_app(main_engine, event_engine)
    tornado_app.listen(9082)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
