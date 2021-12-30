
from tornado.web import RequestHandler
from server.handler.handler import HandlerBase

def write_error(reuqestHanlder:HandlerBase, status: int, message: str):
    reuqestHanlder.set_status(status_code=status, reason=message)
    reuqestHanlder.write_error(status_code=status)