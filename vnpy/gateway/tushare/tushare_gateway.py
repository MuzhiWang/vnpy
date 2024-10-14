from datetime import timedelta
from typing import Dict, List

import tushare as ts
from vnpy.api.rest import RestClient
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import CancelRequest, OrderRequest, SubscribeRequest, HistoryRequest, BarData
from vnpy.event.engine import EventEngine

GATEWAY_NAME = "TUSHARE"

# 数据频率映射
INTERVAL_VT2TS: Dict[Interval, str] = {
    Interval.MINUTE: "1min",
    Interval.HOUR: "60min",
    Interval.DAILY: "D",
}

# 交易所映射
EXCHANGE_VT2TS: Dict[Exchange, str] = {
    Exchange.CFFEX: "CFX",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "ZCE",
    Exchange.DCE: "DCE",
    Exchange.INE: "INE",
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
}

# 时间调整映射
INTERVAL_ADJUSTMENT_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()
}

# # 中国上海时区
# CHINA_TZ = ZoneInfo("Asia/Shanghai")

_date_format = '%Y%m%d'

class TushareGateway(BaseGateway):

    default_setting = {
        "key": "",
        "secret": "",
        "session_number": 3,
        "proxy_host": "",
        "proxy_port": 0,
    }

    exchanges = [
        Exchange.SSE,
        Exchange.SZSE
    ]

    def __init__(self, event_engine: EventEngine):
        super().__init__(event_engine, GATEWAY_NAME)
        self.client = TushareRestApi(self)


    def connect(self, setting: dict) -> None:
        self.client.start()

    def close(self) -> None:
        pass

    def subscribe(self, req: SubscribeRequest) -> None:
        raise NotImplementedError

    def send_order(self, req: OrderRequest) -> str:
        raise NotImplementedError

    def cancel_order(self, req: CancelRequest) -> None:
        raise NotImplementedError

    def query_account(self) -> None:
        raise NotImplementedError

    def query_position(self) -> None:
        raise NotImplementedError()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        return self.client.query_history(req)



class TushareRestApi(RestClient):

    def __init__(self, gateway: TushareGateway):
        self._gateway = gateway

        token = gateway.default_setting["key"]
        token = '2fe3395cd410fb8ef0c467b5f5c5e871a69050f7d1ba434299d7ceab'
        if token is None:
            return Exception("null token")

        # super.init()
        self._pro = ts.pro_api(token)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        start_date = req.start.date().strftime(_date_format)
        end_date = req.end.date().strftime(_date_format)
        df = self._pro.query(
            api_name='daily',
            fields=[
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount"
            ],
            start_date=start_date,
            end_date=end_date,
            ts_code='000001.SZ'
        )

        res = list()
        for i in df.index:
            res.append(BarData(
                symbol='',
                exchange=req.exchange,
                interval='',
                datetime=df['trade_date'][i],
                open_price=df['open'][i],
                high_price=df['high'][i],
                low_price=df['low'][i],
                close_price=df['close'][i],
                volume=df['vol'][i],
                # open_interest=row.get("open_interest", 0),
                gateway_name="TUSHARE"
            ))

        return res

    def query_stock_list(self, ):
        pass
