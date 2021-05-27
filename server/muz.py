import tornado.ioloop
import tornado.web
import threading
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


class MainHandler(tornado.web.RequestHandler):
    def initialize(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine

    def get(self):
        req = HistoryRequest(
            symbol="BTC-USD",
            exchange=Exchange.COINBASE,
            interval=Interval.MINUTE,
            start=datetime(
                year=2019,
                month=8,
                day=8,
            ),
            end=datetime(
                year=2019,
                month=8,
                day=10,
            ),
        )

        res = self.main_engine.query_history(req, "COINBASE")
        res_arr = []
        for d in res:
            res_arr.append({
                "high": d.high_price,
                "low": d.low_price,
                "open": d.open_price,
                "close": d.close_price,
            })
        res_dic = {
            "res": res_arr
        }

        self.write(res_dic)
        self.write("Hello, world")


def start_vnpy_app(main_engine: MainEngine, event_engine: EventEngine):
    """Start VN Trader"""
    qapp = create_qapp()

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


def start_tornado_app(main_engine: MainEngine, event_engine: EventEngine):
    return tornado.web.Application(
        [
            (r"/test", MainHandler, {
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
