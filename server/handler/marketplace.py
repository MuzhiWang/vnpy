from server.handler.handler import HandlerBase
from server.mapper.mapper import *
from vnpy.trader.object import HistoryRequest, BarData, TradeData
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
            self.set_status(500, "invalid request body")
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
                "id" not in data or \
                "secret" not in data or \
                "server" not in data or \
                "passphrase" not in data:
            self.set_status(500, "invalid request body")
            return

        id = data["id"]
        secret = data["secret"]
        server = data["server"]
        ps = data["passphrase"]
        
        coinbaseGw: CoinbaseGateway = self.main_engine.get_gateway("COINBASE")
        coinbaseGw.connect({
            "ID": id,
            "Secret": secret,
            "server": server,
            "passphrase": ps,
            "会话数": 3,
            "proxy_host": "",
            "proxy_port": ""
        })
