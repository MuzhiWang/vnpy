import json
import tornado.ioloop
import tornado.web
import threading
import time
import sys
import argparse
from datetime import datetime, timedelta

from vnpy.event import EventEngine
from vnpy.gateway.polygon import PolygonGateway
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
# from vnpy.gateway.ctp import CtpGateway
from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.tushare import TushareGateway
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
from server.handler.backtester import GetBacktestResultHandler, RunBacktestHandler, StopBacktestHandler
from server.handler.log_event import LogEventWebSocketHandler

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
            (r"/backtester/result", GetBacktestResultHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/backtester/run", RunBacktestHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/backtester/stop", StopBacktestHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/account/connect", AccountConnectHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            }),
            (r"/event_log", LogEventWebSocketHandler, {
                "main_engine": main_engine,
                "event_engine": event_engine,
            })
        ]
    )


def main():
    parser = argparse.ArgumentParser(description="muz server")
    parser.add_argument(
        "--option",
        choices=["server", "with-app", "test"],
        required=True,
        help="server: start the server, with-app: start the server with vnpy app, test: test the server"
    )

    try:
        event_engine = EventEngine()
        main_engine = MainEngine(event_engine)

        # main_engine.add_gateway(CtpGateway)
        main_engine.add_gateway(BinanceGateway)
        main_engine.add_gateway(CoinbaseGateway)
        main_engine.add_gateway(TushareGateway)
        main_engine.add_gateway(PolygonGateway)

        main_engine.add_app(CtaStrategyApp)
        main_engine.add_app(CtaBacktesterApp)
        main_engine.add_app(AlgoTradingApp)
        main_engine.add_app(DataRecorderApp)
        main_engine.add_app(PortfolioManagerApp)
        main_engine.add_app(PortfolioStrategyApp)
        main_engine.add_app(ChartWizardApp)

        # tushare_gw = main_engine.get_gateway('TUSHARE')
        # from vnpy.trader.object import HistoryRequest
        # res = tushare_gw.query_history(HistoryRequest(
        #     start=datetime.fromisoformat('2022-10-10'),
        #     end=datetime.fromisoformat('2023-05-10'),
        #     symbol='xxx',
        #     exchange=Exchange.SSE,
        # ))

        # print("\n=================================\n")
        # print(res)

        # import akshare as ak
        # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date='20210907',
        #                                         adjust="")
        # print("\n================ akshare =================\n")
        # print(stock_zh_a_hist_df)

        args = parser.parse_args()
        print(args)
        if args.option == "server":
            pass
        elif args.option == "with-app":
            vnpy_thread = threading.Thread(target=start_vnpy_app, args=[main_engine, event_engine], daemon=True)
            vnpy_thread.start()
        elif args.option == "test":
            test_1(main_engine, event_engine)
            return
        else:
            print("invalid option")
            sys.exit(1)

        if args.option == "server":
            tornado_app = start_tornado_app(main_engine, event_engine)
            tornado_app.listen(9082)
            tornado.ioloop.IOLoop.instance().add_timeout(
                timedelta(seconds=1),
                LogEventWebSocketHandler.write_to_clients)

            print("tornado service started")
            tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        sys.exit(0)


def test_1(main_engine: MainEngine, event_engine: EventEngine):
    main_engine.get_gateway(PolygonGateway.GatewayName).connect({})


if __name__ == "__main__":
    main()
