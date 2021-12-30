import datetime
import json
from server.mapper.mapper import *
from server.utils.tornado_utils import *
from vnpy.app.cta_backtester import CtaBacktesterApp, BacktesterEngine, APP_NAME as CtaBacktesterAppName
from server.handler.handler import HandlerBase
from vnpy.trader.object import HistoryRequest, BarData, TradeData
import vnpy.trader.utility as TradeUtility

class GetBacktestResultHandler(HandlerBase):
    def get(self):
        ctaBTApp: BacktesterEngine = self.main_engine.get_engine(CtaBacktesterAppName)
        all_trades: list[TradeData] = ctaBTApp.get_all_trades()
        history: list[BarData] = ctaBTApp.get_history_data()

        history_arr = []
        trades_arr = []
        for d in history:
            history_arr.append({
                "ts": d.datetime.timestamp(),
                "H": d.high_price,
                "L": d.low_price,
                "O": d.open_price,
                "C": d.close_price,
                "V": d.volume
            })
        for t in all_trades:
            trades_arr.append({
                "price": t.price,
                "vol": t.volume,
                "ts": t.datetime.timestamp(),
                "dir": t.direction.name,
                "oid": t.orderid,
                "tid": t.tradeid,
                "offset": t.offset.name
            })
        res_dic = {
            "history_data": history_arr,
            "trades_data": trades_arr
        }

        self.write(res_dic)


class RunBacktestHandler(HandlerBase):
    def post(self):
        data = json.loads(self.request.body)
        if data is None or \
                "class_name" not in data or \
                "exchange" not in data or \
                "interval" not in data or \
                "start_ts" not in data or \
                "end_ts" not in data or \
                "rate" not in data or \
                "slippage" not in data or \
                "size" not in data or \
                "pricetick" not in data or \
                "capital" not in data or \
                "inverse" not in data or \
                "setting" not in data:
            write_error(self, 400, "invalid request")
            return

        class_name: str = data["class_name"]
        symbol = data["symbol"]
        interval = map_stock_interval_to_internal(data["interval"])
        exchange = map_stock_exchange_to_internal(data["exchange"])
        vt_symbol: str = TradeUtility.generate_vt_symbol(symbol=symbol, exchange=exchange)
        start: datetime = data["start_ts"]
        end: datetime = data["end_ts"]
        rate: float = data["rate"]
        slippage: float = data["slippage"]
        size: int = data["size"]
        pricetick: float = data["pricetick"]
        capital: int = data["capital"]
        inverse: bool = data["inverse"]
        setting: dict = data["setting"]
        
        print("run backtesting request: {}".format(data))
        
        ctaBTEngine: BacktesterEngine = self.main_engine.get_engine(CtaBacktesterAppName)
        ctaBTEngine.init_engine()
        self.register_events()
        succeeded = ctaBTEngine.start_backtesting(
            class_name=class_name,
            vt_symbol=vt_symbol,
            interval=interval,
            start=start,
            end=end,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=capital,
            inverse=inverse,
            setting=setting,
        )
        
        res_dic = {
            "run_backtesting_succceeded": succeeded
        }
        self.write(res_dic)
    
    
    def register_events(self):
        # self.event_engine.register(
        #     EVENT_CTA_STRATEGY, callable
        # )
        pass
