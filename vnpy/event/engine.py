"""
Event-driven framework of vn.py framework.
"""

import re

from collections import defaultdict
from queue import Empty, Queue
from threading import Thread
from time import sleep, time
from typing import Any, Callable, List
from kafka.errors import TopicAlreadyExistsError

from vnpy.trader.setting import get_settings
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json

EVENT_TIMER = "eTimer"


class Event:
    """
    Event object consists of a type string which is used
    by event engine for distributing event, and a data
    object which contains the real data.
    """

    def __init__(self, type: str, data: Any = None):
        """"""
        self.type: str = type
        self.data: Any = data

    def __str__(self):
        return "type: {}, data: {}".format(self.type, self.data)


# Defines handler function to be used in event engine.
HandlerType = Callable[[Event], None]


class EventEngine:
    """
    Event engine distributes event object based on its type
    to those handlers registered.

    It also generates timer event by every interval seconds,
    which can be used for timing purpose.
    """

    def __init__(self, interval: int = 1):
        """
        Timer event is generated every 1 second by default, if
        interval not specified.
        """
        self._interval: int = interval
        self._queue: Queue = Queue()
        self._active: bool = False
        self._thread: Thread = Thread(target=self._run)
        self._timer: Thread = Thread(target=self._run_timer)
        self._handlers: defaultdict = defaultdict(list)
        self._general_handlers: List = []
        self._log_debug = get_settings()["log_debug"]
        self._log_debug_exclude_events = get_settings()["log_debug_exclude_events"].split(",")

        self._kafka_event_log_topic = "EVENTLOGGG"
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers='localhost:19092',
                client_id='vnpy_muzhi'
            )
            topic_list = []
            topic_list.append(
                NewTopic(
                    name=self._kafka_event_log_topic,
                    num_partitions=1,
                    replication_factor=1
                )
            )
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError:
            print("kafka topic {} already exists".format(self._kafka_event_log_topic))
        
        self._kafka_producer = KafkaProducer(
            bootstrap_servers='localhost:19092',
            # security_protocol="SASL_SSL",
            # ssl_context=context,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def _run(self) -> None:
        """
        Get event from queue and then process it.
        """
        while self._active:
            try:
                event = self._queue.get(block=True, timeout=1)
                self._process(event)
            except Empty:
                pass

    def _process(self, event: Event) -> None:
        """
        First ditribute event to those handlers registered listening
        to this type.

        Then distrubute event to those general handlers which listens
        to all types.
        """
        if event.type in self._handlers:
            [handler(event) for handler in self._handlers[event.type]]

        if self._general_handlers:
            [handler(event) for handler in self._general_handlers]

        # print("log debug ex: {}", self._log_debug_exclude_events)
        if self._log_debug:
            skip = False
            for val in self._log_debug_exclude_events:
                if val != "" and re.match(val, event.type):
                    skip = True
                    break
            if not skip:
                # event_msg = "ts: {}, {}".format(time(), event)
                event_msg = str(event)
                print(event_msg)
                self._kafka_producer.send(self._kafka_event_log_topic, event_msg)


    def _run_timer(self) -> None:
        """
        Sleep by interval second(s) and then generate a timer event.
        """
        while self._active:
            sleep(self._interval)
            event = Event(EVENT_TIMER)
            self.put(event)

    def start(self) -> None:
        """
        Start event engine to process events and generate timer events.
        """
        self._active = True
        self._thread.start()
        self._timer.start()

    def stop(self) -> None:
        """
        Stop event engine.
        """
        self._active = False
        self._timer.join()
        self._thread.join()

    def put(self, event: Event) -> None:
        """
        Put an event object into event queue.
        """
        self._queue.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
        """
        Register a new handler function for a specific event type. Every
        function can only be registered once for each event type.
        """
        handler_list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: HandlerType) -> None:
        """
        Unregister an existing handler function from event engine.
        """
        handler_list = self._handlers[type]

        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handlers.pop(type)

    def register_general(self, handler: HandlerType) -> None:
        """
        Register a new handler function for all event types. Every
        function can only be registered once for each event type.
        """
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler: HandlerType) -> None:
        """
        Unregister an existing general handler function.
        """
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)
    