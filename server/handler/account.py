import time
from server.handler.handler import HandlerBase
from server.utils.tornado_utils import write_error
from vnpy.gateway.coinbase import CoinbaseGateway
import json

class AccountConnectHandler(HandlerBase):
    def post(self):
        data = json.loads(self.request.body)
        if data is None or \
                "id" not in data or \
                "secret" not in data or \
                "server" not in data or \
                "passphrase" not in data:
            write_error(self, 400, "invalid request")
            return

        id = data["id"]
        secret = data["secret"]
        server = data["server"]
        ps = data["passphrase"]
        
        coinbaseGw: CoinbaseGateway = self.main_engine.get_gateway("COINBASE")
        coinbaseGw.connect({
            "ID": id,
            "Secret": secret,
            "server": server,
            "passphrase": ps,
            "会话数": 3,
            "proxy_host": "",
            "proxy_port": ""
        })
        
        self.write("account connected")
