import json
import tornado.ioloop
import tornado.web
import threading
import time
import sys
import os
import argparse
from datetime import datetime, timedelta

from vnpy.event import EventEngine
from vnpy.gateway.polygon import PolygonGateway
from vnpy.trader.engine import MainEngine
from vnpy.trader.object import HistoryRequest
from vnpy.trader.ui import MainWindow, create_qapp
# from vnpy.gateway.ctp import CtpGateway
from vnpy.gateway.binance import BinanceGateway
from vnpy.gateway.tushare import TushareGateway
from vnpy.gateway.coinbase import CoinbaseGateway
from vnpy.app.cta_strategy import CtaStrategyApp
from vnpy.app.cta_backtester import CtaBacktesterApp, BacktesterEngine, APP_NAME as CtaBacktesterAppName
from vnpy.app.algo_trading import AlgoTradingApp
from vnpy.app.data_recorder import DataRecorderApp, APP_NAME
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
    sub_parsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # Add a subcommand called "server"
    server_parser = sub_parsers.add_parser("server", help="Manage the server")
    server_parser.add_argument("--only", action="store_true", help="Start the server only")
    server_parser.add_argument("--with-app", action="store_true", help="With app")
    # server_parser.add_argument("--restart", action="store_true", help="Restart the server")

    # Add a subcommand called "test"
    test_parser = sub_parsers.add_parser("test", help="Run tests")
    test_parser.add_argument("--unit", type=str, default="null", help="Run unit tests")
    # test_parser.add_argument("--integration", action="store_true", help="Run integration tests")

    args = parser.parse_args()
    print(args)

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

        # print("\n=================================\n")
        # print(res)

        # import akshare as ak
        # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", end_date='20210907',
        #                                         adjust="")
        # print("\n================ akshare =================\n")
        # print(stock_zh_a_hist_df)

        # Handle each command and its options
        if args.command == "server":
            if args.only:
                pass
            elif args.with_app:
                vnpy_thread = threading.Thread(target=start_vnpy_app, args=[main_engine, event_engine], daemon=True)
                vnpy_thread.start()

            # Start tornado app
            tornado_app = start_tornado_app(main_engine, event_engine)
            tornado_app.listen(9082)
            tornado.ioloop.IOLoop.instance().add_timeout(
                timedelta(seconds=1),
                LogEventWebSocketHandler.write_to_clients)

            print("tornado service started")
            tornado.ioloop.IOLoop.instance().start()
        elif args.command == "test":
            if args.unit:
                if args.unit == "polygon":
                    test_polygon(main_engine, event_engine)
                elif args.unit == "tushare":
                    test_tushare(main_engine, event_engine)

    except KeyboardInterrupt:
        # sys.exit(0)
        os._exit(0)


def test_polygon(main_engine: MainEngine, event_engine: EventEngine):
    gw = main_engine.get_gateway(PolygonGateway.GatewayName)
    gw.connect({})
    req = HistoryRequest(
        symbol="AAPL",
        exchange=Exchange.NASDAQ,
        start=datetime(2023, 1, 1),
        end=datetime(2023, 1, 31)
    )
    res = gw.query_history(req)
    print(res)

    recorder_engine = main_engine.get_engine(APP_NAME)
    # recorder_engine.start()

    for bar in res:
        recorder_engine.record_bar(bar)

    # gw.close()
    # recorder_engine.close()

def test_tushare(main_engine: MainEngine, event_engine: EventEngine):
    tushare_gw = main_engine.get_gateway('TUSHARE')
    from vnpy.trader.object import HistoryRequest
    res = tushare_gw.query_history(HistoryRequest(
        start=datetime.fromisoformat('2022-10-10'),
        end=datetime.fromisoformat('2023-05-10'),
        symbol='xxx',
        exchange=Exchange.SSE,
    ))
    print(res)

if __name__ == "__main__":
    main()
