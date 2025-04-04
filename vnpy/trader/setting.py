"""
Global setting of VN Trader.
"""

import os
from logging import CRITICAL
from typing import Dict, Any
from tzlocal import get_localzone

from .utility import load_json

SETTINGS: Dict[str, Any] = {
    "font.family": "Arial",
    "font.size": 12,

    "log.active": True,
    "log.level": CRITICAL,
    "log.console": True,
    "log.file": True,

    "email.server": "smtp.qq.com",
    "email.port": 465,
    "email.username": "",
    "email.password": "",
    "email.sender": "",
    "email.receiver": "",

    "rqdata.username": "",
    "rqdata.password": "",

    "database.timezone": get_localzone().zone,
    "database.driver": "mongodb",                # see database.Driver
    "database.database": "test",         # for sqlite, use this as filepath
    "database.host": "localhost",
    "database.port": 27017,
    "database.user": "",
    "database.password": "",
    "database.authentication_source": "admin",  # for mongodb

    "genus.parent_host": "",
    "genus.parent_port": "",
    "genus.parent_sender": "",
    "genus.parent_target": "",
    "genus.child_host": "",
    "genus.child_port": "",
    "genus.child_sender": "",
    "genus.child_target": "",

    "log_debug": True,
    "log_debug_exclude_events": "eTimer,eAccount.*,eContract.", # split by comma, e.g. "eAccount.,eTick."
    "websocket_interval_ms": 200,
    
    "kafka_broker_host_port": "localhost:19092",
}

# Load global setting from json file.
SETTING_FILENAME: str = "vt_setting.json"
SETTINGS.update(load_json(SETTING_FILENAME))

docker_kafka_host = os.getenv("DOCKER_KAFKA_HOST_PORT")
if docker_kafka_host is not None and docker_kafka_host != "":
    SETTINGS["kafka_broker_host_port"] = docker_kafka_host

if SETTINGS["database.host"] == "localhost":
    docker_host = os.getenv("DOCKER_HOST")
    if docker_host is not None and docker_host != "":
        SETTINGS["database.host"] = docker_host


def get_settings(prefix: str = "") -> Dict[str, Any]:
    prefix_length = len(prefix)
    return {k[prefix_length:]: v for k, v in SETTINGS.items() if k.startswith(prefix)}
