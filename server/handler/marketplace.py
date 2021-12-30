from server.handler.handler import HandlerBase
from server.mapper.mapper import *
from server.utils.tornado_utils import write_error
from vnpy.app.cta_backtester.engine import BacktesterEngine
from vnpy.app.cta_backtester.engine import APP_NAME as BT_ENGINE_APP_NAME
from vnpy.trader.object import HistoryRequest, BarData, TradeData
import vnpy.trader.utility as TradeUtility
from datetime import datetime
import json

class StockDataHandler(HandlerBase):
    def post(self):
        data = json.loads(self.request.body)
        if data is None or \
                "symbol" not in data or \
                "interval" not in data or \
                "exchange" not in data or \
                "start_ts" not in data or \
                "end_ts" not in data:
            write_error(self, 400, "invalid request")
            return
        
        symbol = data["symbol"]
        interval = map_stock_interval_to_internal(data["interval"])
        exchange = map_stock_exchange_to_internal(data["exchange"])
        start_ts = data["start_ts"]
        end_ts = data["end_ts"]

        start_dt = datetime.utcfromtimestamp(start_ts)
        end_dt = datetime.utcfromtimestamp(end_ts)

        print("start_dt:{}, end_dt:{}".format(start_dt, end_dt))

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


class DownloadDataHandler(HandlerBase):
    def post(self):
        data = json.loads(self.request.body)
        if data is None or \
                "symbol" not in data or \
                "exchange" not in data or \
                "interval" not in data or \
                "start_ts" not in data or \
                "end_ts" not in data:
            write_error(self, 400, "invalid request")
            return

        symbol = data["symbol"]
        interval = map_stock_interval_to_internal(data["interval"])
        exchange = map_stock_exchange_to_internal(data["exchange"])
        start_ts = data["start_ts"]
        end_ts = data["end_ts"]
        
        start_dt = datetime.utcfromtimestamp(start_ts)
        end_dt = datetime.utcfromtimestamp(end_ts)

        print("start_dt:{}, end_dt:{}".format(start_dt, end_dt))

        btEngine: BacktesterEngine = self.main_engine.get_engine(BT_ENGINE_APP_NAME)
        succeeded = btEngine.start_downloading(
            vt_symbol=TradeUtility.generate_vt_symbol(symbol=symbol, exchange=exchange),
            interval=interval,
            start=start_dt,
            end=end_dt
        )
        
        res_dic = {
            "download_succceeded": succeeded
        }
        self.write(res_dic)
