from vnpy.app.cta_backtester import CtaBacktesterApp, BacktesterEngine, APP_NAME as CtaBacktesterAppName
from server.handler.handler import HandlerBase
from vnpy.trader.object import HistoryRequest, BarData, TradeData

class BacktesterHandler(HandlerBase):
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
