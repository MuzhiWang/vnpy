from vnpy.event import EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.object import (
    TickData, OrderData, TradeData, PositionData, AccountData,
    ContractData, LogData, OrderRequest, CancelRequest,
    SubscribeRequest, HistoryRequest, BarData
)
from datetime import datetime
from typing import List
from vnpy.trader.gateway import BaseGateway
from polygon import RESTClient
from vnpy.api.rest import RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.utility import load_json


class PolygonGateway(BaseGateway):
    GatewayName = "POLYGON"
    setting_filename = "polygon_settings.json"
    api_key_id = "api_key"

    """
    Gateway for fetching US stock data using Polygon.io API.
    """
    default_setting = {
        api_key_id: "Your API Key",
    }

    exchanges = ["NYSE", "NASDAQ"]

    def __init__(self, event_engine: EventEngine):
        super().__init__(event_engine, PolygonGateway.GatewayName)

        self.rest_api = PolygonRestApi(self)
        self.ws_api = PolygonWebsocketApi(self)
        self._local_settings = load_json(PolygonGateway.setting_filename)
        self._api_key = self._local_settings[PolygonGateway.api_key_id]

    def get_local_settings(self) -> dict:
        return self._local_settings

    def connect(self, setting: dict) -> None:
        """
        Connect to the Polygon.io API.
        """
        self._api_key = setting.get(PolygonGateway.api_key_id)

        if not self._api_key:
            self._api_key = self.get_local_settings()[PolygonGateway.api_key_id]
            self.write_log("no api key from setting, load from local cache")

        if not self._api_key:
            self.write_log("no api key found.")
            return

        self.rest_api.connect(self._api_key)
        self.write_log("Connected to Polygon Gateway.")
        # self.query_account()
        # self.query_position()
        req = HistoryRequest(
            symbol="AAPL",
            exchange=Exchange.NASDAQ,
            start=datetime(2023, 1, 1),
            end=datetime(2023, 1, 31)
        )
        res = self.query_history(req)
        self.write_log(res)

    def close(self) -> None:
        """
        Close the connection.
        """
        self.rest_api.stop()
        self.ws_api.stop()
        self.write_log("Connection to Polygon Gateway closed.")

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe to tick data updates.
        """
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """
        Send a new order to the server (mocked for now).
        """
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel an existing order (mocked for now).
        """
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """
        Query account balance (mocked for now).
        """
        self.rest_api.query_account()

    def query_position(self) -> None:
        """
        Query holding positions (mocked for now).
        """
        self.rest_api.query_position()

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        Query historical bar data from the API.
        """
        return self.rest_api.query_history(req)


class PolygonRestApi(RestClient):
    """
    REST API for Polygon Gateway using Polygon.io Python client.
    """

    def __init__(self, gateway: PolygonGateway):
        super().__init__()
        self.gateway = gateway
        self.api_key = ""
        self.client = None

    def connect(self, api_key: str) -> None:
        """
        Connect to REST API server.
        """
        self.api_key = api_key
        self.client = RESTClient(self.api_key)
        # self.start(3)
        self.gateway.write_log("REST API connected.")

    def query_account(self) -> None:
        """
        Query account balance (mocked for now).
        """
        account = AccountData(
            accountid="US_ACCOUNT",
            balance=100000.0,
            available=100000.0,
            gateway_name=self.gateway.gateway_name
        )
        self.gateway.on_account(account)

    def query_position(self) -> None:
        """
        Query holding positions (mocked for now).
        """
        position = PositionData(
            symbol="AAPL",
            exchange="NASDAQ",
            direction="LONG",
            volume=10,
            gateway_name=self.gateway.gateway_name
        )
        self.gateway.on_position(position)

    def send_order(self, req: OrderRequest) -> str:
        """
        Send a new order to the server (mocked for now).
        """
        order_id = f"US_{req.symbol}_ORDER"
        order = OrderData(
            symbol=req.symbol,
            exchange=req.exchange,
            orderid=order_id,
            direction=req.direction,
            price=req.price,
            volume=req.volume,
            gateway_name=self.gateway.gateway_name,
        )

        self.gateway.on_order(order)
        return order_id

    def cancel_order(self, req: CancelRequest) -> None:
        """
        Cancel an existing order (mocked for now).
        """
        self.gateway.write_log(f"Cancel order request received for orderid: {req.orderid}")

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        Query historical bar data from the Polygon.io API using the Polygon Python client.
        """
        try:
            response = self.client.get_aggs(ticker=req.symbol, multiplier=1, timespan="day", from_=req.start,
                                            to=req.end, adjusted=True, sort="asc")
            bars = []

            for item in response:
                bar = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=datetime.fromtimestamp(item.timestamp / 1000),
                    open_price=item.open,
                    high_price=item.high,
                    low_price=item.low,
                    close_price=item.close,
                    volume=item.volume,
                    gateway_name=self.gateway.gateway_name
                )
                bars.append(bar)
            return bars

        except Exception as e:
            self.gateway.write_log(f"Failed to fetch historical data: {str(e)}")
            return []


class PolygonWebsocketApi(WebsocketClient):
    """
    WebSocket API for Polygon Gateway (mocked for now).
    """

    def __init__(self, gateway: PolygonGateway):
        super().__init__()
        self.gateway = gateway
        self.subscribed = {}

    def connect(self) -> None:
        """
        Connect to WebSocket server (mocked for now).
        """
        self.gateway.write_log("WebSocket API connected (mocked).")

    def subscribe(self, req: SubscribeRequest) -> None:
        """
        Subscribe to tick data updates (mocked for now).
        """
        self.subscribed[req.symbol] = req
        self.gateway.write_log(f"Subscribed to tick data for symbol: {req.symbol} (mocked)")

    def on_connected(self) -> None:
        """
        Callback when WebSocket is connected (mocked for now).
        """
        self.gateway.write_log("WebSocket API connection established (mocked).")
        for req in self.subscribed.values():
            self.subscribe(req)

    def on_disconnected(self) -> None:
        """
        Callback when WebSocket is disconnected (mocked for now).
        """
        self.gateway.write_log("WebSocket API connection lost (mocked).")
