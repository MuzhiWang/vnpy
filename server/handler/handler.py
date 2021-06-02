from abc import ABC

import tornado.web

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine


class HandlerBase(tornado.web.RequestHandler, ABC):
    def initialize(self, main_engine: MainEngine, event_engine: EventEngine):
        self.main_engine = main_engine
        self.event_engine = event_engine

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header("Access-Control-Allow-Headers", "access-control-allow-origin,authorization,content-type,"
                                                        "x-requested-with")

    def options(self):
        # no body
        self.set_status(204)
        self.finish()
