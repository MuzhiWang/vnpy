from typing import List

import akshare as ak
from vnpy.api.rest import RestClient
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import CancelRequest, OrderRequest, SubscribeRequest, HistoryRequest, BarData
from vnpy.event.engine import EventEngine


GATEWAY_NAME = "AKSHARE"

class AKShareGateway(BaseGateway):


    def __init__(self, event_engine: EventEngine):
        super().__init__(event_engine, GATEWAY_NAME)

    def connect(self, setting: dict) -> None:
        pass

    def close(self) -> None:
        pass

    def subscribe(self, req: SubscribeRequest) -> None:
        pass

    def send_order(self, req: OrderRequest) -> str:
        pass

    def cancel_order(self, req: CancelRequest) -> None:
        pass

    def query_account(self) -> None:
        pass

    def query_position(self) -> None:
        pass

    def query_stock_list(self) -> List[str]:

        pass