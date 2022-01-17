import asyncio
from datetime import timedelta
import threading
import json
from typing import Awaitable, List, Optional, Union
from numpy import array
import tornado
from tornado.websocket import WebSocketHandler
from kafka.consumer.group import KafkaConsumer
from collections import deque
import tornado.ioloop

from server.handler.handler import WsHandlerBase
from vnpy.event.engine import EventEngine
from vnpy.trader.engine import MainEngine

class LogEventWebSocketHandler(WsHandlerBase):
    
    _clients: List[WebSocketHandler] = []
    _consumer: KafkaConsumer = KafkaConsumer("EVENTLOGGG",
                            bootstrap_servers='localhost:19092',
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                        )
    _event_queue: deque = deque(["initialize info",], maxlen=10000)
    # _io_loop = tornado.ioloop.IOLoop.current()
    # asyncio.set_event_loop(io_loop)
    
    def initialize(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine
        self._consumer.subscribe(["EVENTLOGGG"])
        self.start_kafka()
    
    def open(self, *args: str, **kwargs: str) -> Optional[Awaitable[None]]:
        print("log event websocket open")
        self._clients.append(self)
        # self._io_loop.asyncio_loop.
    
    def on_message(self, message: Union[str, bytes]) -> Optional[Awaitable[None]]:
        print(message)
        self.write_message(message=message)
    
    def on_close(self) -> None:
        print("log event websocket closed")
        self._clients.remove(self)
        return super().on_close()
    
    def check_origin(self, origin: str) -> bool:
        return True

    def start_kafka(cls):
        k_consumer_thread = threading.Thread(target=cls._kafka_consuming)
        k_consumer_thread.start()
    
    @classmethod
    def write_to_clients(cls):
        try:
            print("writing to {} clients".format(len(cls._clients)))
            for idx, client in enumerate(cls._clients):
                print("start client {}".format(idx))
                while(cls._event_queue):
                    client._write_message(cls._event_queue.pop())
        finally:
            tornado.ioloop.IOLoop.instance().add_timeout(
                timedelta(seconds=1),
                LogEventWebSocketHandler.write_to_clients)
    
    def _write_message(self, msg: bytes):
        print("$$$$$$$$$$$$$$$$$")
        # bytt = bytes("hahahahahah testjklsajfkldsa jkjfklsaj", "utf-8")
        # print(bytt)
        print(msg)
        self.write_message(msg)
    
    @classmethod
    def _kafka_consuming(cls):
        for msg in cls._consumer:
            print("*********************************")
            bytt = bytes(json.dumps(msg), "utf-8")
            cls._event_queue.appendleft(bytt)
            print(bytt)
            