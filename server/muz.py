import tornado.ioloop
import tornado.web
import threading
import time
import sys
from datetime import datetime

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
# from vnpy.gateway.ctp import CtpGateway
from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.coinbase import CoinbaseGateway
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_backtester import CtaBacktesterApp, BacktesterEngine, APP_NAME as CtaBacktesterAppName
from vnpy.app.algo_trading import AlgoTradingApp
from vnpy.app.data_recorder import DataRecorderApp
from vnpy.app.portfolio_manager import PortfolioManagerApp
from vnpy.app.portfolio_strategy import PortfolioStrategyApp
from vnpy.app.chart_wizard import ChartWizardApp

from server.mapper.mapper import *
from server.handler.marketplace import DownloadDataHandler, StockDataHandler
from server.handler.account import AccountConnectHandler
from server.handler.backtester import BacktesterHandler

def start_vnpy_app(main_engine: MainEngine, event_engine: EventEngine):
    """Start VN Trader"""
    qapp = create_qapp()

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


def start_tornado_app(main_engine: MainEngine, event_engine: EventEngine):
    return tornado.web.Application(
        [
            (r"/marketplace/stock_data", StockDataHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/marketplace/download_data", DownloadDataHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/backtester", BacktesterHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/account/connect", AccountConnectHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
        ]
    )


def main():
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # main_engine.add_gateway(CtpGateway)
    main_engine.add_gateway(BinanceGateway)
    main_engine.add_gateway(CoinbaseGateway)

    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(AlgoTradingApp)
    main_engine.add_app(DataRecorderApp)
    main_engine.add_app(PortfolioManagerApp)
    main_engine.add_app(PortfolioStrategyApp)
    main_engine.add_app(ChartWizardApp)

    args = sys.argv
    print(args)
    if len(args) > 1 and args[1].lower() == "true":
        vnpy_thread = threading.Thread(target=start_vnpy_app, args=[main_engine, event_engine], daemon=True)
        vnpy_thread.start()

    tornado_app = start_tornado_app(main_engine, event_engine)
    tornado_app.listen(9082)
    print("tornado service started")
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
