# encoding: UTF-8

'''
火币交易接口
'''

from __future__ import print_function

import base64
import hashlib
import hmac
import json
import re
from urllib import parse
import zlib
from copy import copy
from threading import Lock
from datetime import datetime
import requests

from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getTempPath, getJsonPath

REST_HOST = 'https://api.hbdm.com'
WEBSOCKET_MARKET_HOST = 'wss://www.hbdm.com/ws'  # 行情
WEBSOCKET_TRADE_HOST = 'wss://api.hbdm.com/notification'  # 资金和委托

# 委托状态类型映射
statusMapReverse = {}
statusMapReverse[3] = STATUS_NOTTRADED
statusMapReverse[4] = STATUS_PARTTRADED
statusMapReverse[5] = STATUS_CANCELLED
statusMapReverse[6] = STATUS_ALLTRADED
statusMapReverse[7] = STATUS_CANCELLED

ORDERTYPE_VT2HBDM = {
    PRICETYPE_MARKETPRICE: "opponent",
    PRICETYPE_LIMITPRICE: "limit",
}
ORDERTYPE_HBDM2VT = {v: k for k, v in ORDERTYPE_VT2HBDM.items()}
ORDERTYPE_HBDM2VT['1'] = PRICETYPE_LIMITPRICE
ORDERTYPE_HBDM2VT['3'] = PRICETYPE_MARKETPRICE

ORDERTYPE_VT2HBDM = {v: k for k, v in ORDERTYPE_VT2HBDM.items()}

DIRECTION_VT2HBDM = {
    DIRECTION_LONG: 'bull',
    DIRECTION_SHORT: 'sell'
}

DIRECTION_HBDM2VT = {v: k for k, v in DIRECTION_VT2HBDM.items()}

OFFSET_VT2HBDM = {
    OFFSET_OPEN: 'open',
    OFFSET_CLOSE: 'close'
}

OFFSET_HBDM2VT = {v: k for k, v in OFFSET_VT2HBDM.items()}

CONTRACT_TYPE_MAP = {
    "this_week": "CW",
    "next_week": "NW",
    "this_quarter": "CQ"
}

HISTORY_INTERVEL = {
    'MINUTE': '1min',
    'HOUR': '60min',
    'DAILY': '1day'

}

symbol_type_map = {}

# ----------------------------------------------------------------------
def _split_url(url):
    """
    将url拆分为host和path
    :return: host, path
    """
    m = re.match('\w+://([^/]*)(.*)', url)
    if m:
        return m.group(1), m.group(2)


# ----------------------------------------------------------------------
def createSignature(apiKey, method, host, path, secretKey, getParams=None):
    """
    创建签名
    :param getParams: dict 使用GET方法时附带的额外参数(urlparams)
    :return:
    """
    sortedParams = [
        ("AccessKeyId", apiKey),
        ("SignatureMethod", 'HmacSHA256'),
        ("SignatureVersion", "2"),
        ("Timestamp", datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'))
    ]
    if getParams:
        sortedParams.extend(list(getParams.items()))
        sortedParams = list(sorted(sortedParams))
    encodeParams = parse.urlencode(sortedParams)

    payload = [method, host, path, encodeParams]
    payload = '\n'.join(payload)
    payload = payload.encode(encoding='UTF8')

    secretKey = secretKey.encode(encoding='UTF8')

    digest = hmac.new(secretKey, payload, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(digest)
    signature = signature.decode()

    params = dict(sortedParams)
    params["Signature"] = signature
    return params


########################################################################
class HbdmGateway(VtGateway):
    """火币接口"""

    # ----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='HBDM'):
        """Constructor"""
        super(HbdmGateway, self).__init__(eventEngine, gatewayName)

        self.order_count = 0

        self.orderDict = {}
        # self.localOrderDict = {}
        # self.orderLocalDict = {}

        self.restApi = HbdmRestApi(self)
        self.tradeWsApi = HbdmTradeWebsocketApi(self)
        self.marketWsApi = HbdmMarketWebsocketApi(self)

        self.qryEnabled = False  # 是否要启动循环查询

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)

    # ----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath)
        except IOError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'读取连接配置出错，请检查'
            self.onLog(log)
            return

        # 解析json文件
        setting = json.load(f)
        try:
            accessKey = str(setting['accessKey'])
            secretKey = str(setting['secretKey'])
            symbols = setting['symbols']
        except KeyError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'连接配置缺少字段，请检查'
            self.onLog(log)
            return

        # 创建行情和交易接口对象
        self.restApi.connect(symbols, accessKey, secretKey)
        self.tradeWsApi.connect(symbols, accessKey, secretKey)
        self.marketWsApi.connect(symbols, accessKey, secretKey)

        # 初始化并启动查询
        self.initQuery()

    # ----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        pass

    # ----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        return self.restApi.sendOrder(orderReq)

    def sendOrders(self,orderReqs):
        return self.restApi.send_orders(orderReqs)

    # ----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        self.restApi.cancelOrder(cancelOrderReq)

    def query_account(self):
        """"""
        self.restApi.queryAccount()

    def query_position(self):
        """"""
        self.restApi.queryPosition()

    def query_history(self, req):
        """"""
        return self.restApi.query_history(req)

    # ----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.restApi.stop()
        self.tradeWsApi.stop()
        self.marketWsApi.stop()

    # ----------------------------------------------------------------------
    def initQuery(self):
        if self.qryEnabled:
            self.count = 0
            self.eventEngine.register(EVENT_TIMER, self.query)

    # ----------------------------------------------------------------------
    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        self.count += 1
        if self.count < 3:
            return
        self.query_account()
        self.query_position()

    # ----------------------------------------------------------------------
    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    # ----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled

    # ----------------------------------------------------------------------
    def writeLog(self, msg):
        """"""
        log = VtLogData()
        log.logContent = msg
        log.gatewayName = self.gatewayName

        event = Event(EVENT_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)


########################################################################
class HbdmRestApi(RestClient):

    # ----------------------------------------------------------------------
    def __init__(self, gateway):  # type: (VtGateway)->HuobiRestApi
        """"""
        super(HbdmRestApi, self).__init__()

        self.gateway = gateway
        self.gatewayName = gateway.gatewayName

        self.symbols = []
        self.apiKey = ""
        self.apiSecret = ""
        self.signHost = ""

        self.accountid = ''  #

        self.order_count_lock = Lock()
        self.order_count = 10000
        self.connect_time = 0

        self.orderDict = gateway.orderDict
        self.positions = {}

    # ----------------------------------------------------------------------
    def sign(self, request):
        request.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"
        }
        paramsWithSignature = createSignature(self.apiKey,
                                              request.method,
                                              self.signHost,
                                              request.path,
                                              self.apiSecret,
                                              request.params)
        request.params = paramsWithSignature

        if request.method == "POST":
            request.headers['Content-Type'] = 'application/json'

            if request.data:
                request.data = json.dumps(request.data)

        return request

    # ----------------------------------------------------------------------
    def connect(self, symbols, apiKey, apiSecret, sessionCount=3):
        """连接服务器"""
        self.symbols = symbols
        self.apiKey = apiKey
        self.apiSecret = apiSecret

        host, path = _split_url(REST_HOST)
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))
        self.init(REST_HOST)

        self.signHost = host
        self.start(sessionCount)

        self.gateway.writeLog("hbdm REST API启动成功")

        self.queryContract()

    # ----------------------------------------------------------------------
    def queryAccount(self):
        """"""
        self.addRequest('GET', '/v1/account/accounts', self.onQueryAccount)

    def queryPosition(self):
        """"""
        self.addRequest(method="POST",path="/api/v1/contract_position_info",callback=self.onQueryPosition)

   # ----------------------------------------------------------------------
    def queryOrder(self):
        """"""
        for symbol in self.symbols:
            symbol = symbol.split('_')[0]
            data = {
                'symbol': symbol,
            }
            self.addRequest(
                method="POST",
                path="/api/v1/contract_openorders",
                callback=self.on_query_active_order,
                data=data,
                extra=symbol
            )
            # History Orders
            data = {
                "symbol": symbol,
                "trade_type": 0,
                "type": 2,
                "status": 0,
                "create_date": 7
            }

            self.addRequest(
                method="POST",
                path="/api/v1/contract_hisorders",
                callback=self.on_query_history_order,
                data=data,
                extra=symbol
            )

    def queryTrade(self):
        for currency in self.symbols:
            currency = currency.split('_')[0]

            data = {
                "symbol": currency,
                "trade_type": 0,
                "create_date": 7
            }

            self.addRequest(
                method="POST",
                path="/api/v1/contract_matchresults",
                callback=self.on_query_trade,
                data=data,
                extra=currency
            )
    # ----------------------------------------------------------------------
    def queryContract(self):
        """"""
        self.addRequest(
            method="GET",
            path="/api/v1/contract_contract_info",
            callback=self.onQueryContract
        )

    # def query_history(self, req):
    #     """"""
    #     # Convert symbol
    #     contract_type = symbol_type_map.get(req.symbol, "")
    #     buf = [i for i in req.symbol if not i.isdigit()]
    #     symbol = "".join(buf)
    #
    #     ws_contract_type = CONTRACT_TYPE_MAP[contract_type]
    #     ws_symbol = "{}_{}".format(contract_type,ws_contract_type)
    #
    #     # Create query params
    #     params = {
    #         "symbol": ws_symbol,
    #         "period": HISTORY_INTERVEL[req.interval],
    #         "size": 2000
    #     }
    #
    #     # Get response from server
    #     resp = self.request(
    #         "GET",
    #         "/market/history/kline",
    #         params=params
    #     )
    #
    #     # Break if request failed with other status code
    #     history = []
    #
    #     if resp.status_code // 100 != 2:
    #         msg = "获取历史数据失败，状态码：{}，信息：{}".format(resp.status_code,resp.text)
    #         self.gateway.writeLog(msg)
    #     else:
    #         data = resp.json()
    #         if not data:
    #             msg = "获取历史数据为空"
    #             self.gateway.writeLog(msg)
    #         else:
    #             for d in data["data"]:
    #                 dt = datetime.fromtimestamp(d["id"])
    #
    #                 bar = VtBarData()
    #                 bar.symbol = req.symbol
    #                 bar.exchange = req.exchange,
    #                 bar.datetime = dt,
    #                 bar.interval = req.interval,
    #                 bar.volume=d["vol"],
    #                 bar.open=d["open"],
    #                 bar.high=d["high"],
    #                 bar.low=d["low"],
    #                 bar.close=d["close"],
    #                 bar.gatewayName=self.gateway_name
    #
    #                 history.append(bar)
    #
    #             begin = history[0].datetime
    #             end = history[-1].datetime
    #             msg = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
    #             self.gateway.writeLog(msg)
    #
    #     return history

    def request(self,method,path,params):
        url = self.signHost + path
        if method == 'GET':
            ret = requests.get(url,params=params)
            return ret

    # ----------------------------------------------------------------------
    def new_local_orderid(self):
        """"""
        with self.order_count_lock:
            self.order_count += 1
            local_orderid = f"{self.connect_time}{self.order_count}"
            return local_orderid

    def sendOrder(self, orderReq):
        """"""
        local_orderid = self.new_local_orderid()
        order = VtOrderData()
        order.orderID = local_orderid
        order.gatewayName = self.gatewayName
        order.vtOrderID = '.'.join([order.gatewayName,order.orderID])

        direction = DIRECTION_VT2HBDM[orderReq.direction]
        offset = OFFSET_VT2HBDM[orderReq.offset]
        priceType = ORDERTYPE_VT2HBDM[orderReq.priceType]

        data = {
            "contract_code": orderReq.symbol,
            "client_order_id": int(local_orderid),
            'price': str(orderReq.price),
            "volume": int(orderReq.volume),
            "lever_rate": 20,
            "direction": direction,
            "offset": offset,
            "order_price_type": priceType
        }

        self.addRequest(
            method="POST",
            path="/api/v1/contract_order",
            callback=self.onSendOrder,
            data=data,
            extra=orderReq,
            onError=self.on_send_order_error,
            onFailed=self.on_send_order_failed
        )
        # 返回订单号
        return order.vtOrderID

    def send_orders(self, reqs):
        """"""
        orders_data = []
        orders = []
        vt_orderids = []

        for req in reqs:
            local_orderid = self.new_local_orderid()

            order = VtOrderData()
            order.orderID = local_orderid
            order.gatewayName = self.gatewayName
            order.vtOrderID = '.'.join([order.gatewayName, order.orderID])

            order.orderTime = datetime.now().strftime("%H:%M:%S")
            self.gateway.onOrder(order)

            d = {
                "contract_code": req.symbol,
                "client_order_id": int(local_orderid),
                "price": req.price,
                "volume": int(req.volume),
                "direction": DIRECTION_VT2HBDM.get(req.direction, ""),
                "offset": OFFSET_VT2HBDM.get(req.offset, ""),
                "order_price_type": ORDERTYPE_VT2HBDM.get(req.type, ""),
                "lever_rate": 20
            }

            orders_data.append(d)
            orders.append(order)
            vt_orderids.append(order.vtOrderID)

        data = {
            "orders_data": orders_data
        }

        self.addRequest(
            method="POST",
            path="/api/v1/contract_batchorder",
            callback=self.on_send_orders,
            data=data,
            extra=orders,
            on_error=self.on_send_orders_error,
            on_failed=self.on_send_orders_failed
        )
        return vt_orderids

    # ----------------------------------------------------------------------
    def cancelOrder(self, cancelReq):
        """"""
        buf = [i for i in cancelReq.symbol if not i.isdigit()]

        data = {
            "symbol": "".join(buf),
        }

        orderid = int(cancelReq.orderID)
        # if orderid > 1000000:
        #     data["client_order_id"] = orderid
        # else:
        #     data["order_id"] = orderid

        data["client_order_id"] = orderid

        self.addRequest(
            method="POST",
            path="/api/v1/contract_cancel",
            callback=self.onCancelOrder,
            on_failed=self.on_cancel_order_failed,
            data=data,
            extra=cancelReq
        )

    def onQueryAccount(self, data, request):  # type: (dict, Request)->None

        """"""
        if self.check_error(data, "查询账户"):
            return
        for d in data['data']:
            account = VtAccountData()

            account.accountID = d['symbol']
            account.balance = d['margin_balance']
            account.margin = d['margin_frozen']
            account.gatewayName = self.gatewayName

            self.gateway.onAccount(account)

    def onQueryPosition(self,data,request):
        if self.check_error(data, "查询持仓"):
            return

        # Clear all buf data
        for position in self.positions.values():
            position.volume = 0
            position.frozen = 0
            position.price = 0
            position.pnl = 0

        for d in data["data"]:
            key = f"{d['contract_code']}_{d['direction']}"
            position = self.positions.get(key, None)

            if not position:
                position = VtPositionData()
                position.symbol = d["contract_code"]
                position.exchange = EXCHANGE_HBDM
                position.direction= DIRECTION_HBDM2VT[d["direction"]],
                position.gateway_name=self.gateway_name

                self.positions[key] = position

            position.volume = d["volume"]
            position.frozen = d["frozen"]
            position.price = d["cost_hold"]
            position.pnl = d["profit"]

        for position in self.positions.values():
            self.gateway.onPosition(position)

    def on_query_active_order(self,data,request):
        if self.check_error(data, "查询活动委托"):
            return

        for d in data["data"]["orders"]:
            timestamp = d["created_at"]
            dt = datetime.fromtimestamp(timestamp / 1000)
            time = dt.strftime("%H:%M:%S")

            if d["client_order_id"]:
                orderid = d["client_order_id"]
            else:
                orderid = d["order_id"]

            order = VtOrderData()
            order.orderID = orderid
            order.symbol = d["contract_code"]
            order.exchange = EXCHANGE_HBDM
            order.price = d["price"]
            order.totalVolume = d["volume"]
            order.tradedVolume = d["trade_volume"]
            order.priceType = ORDERTYPE_HBDM2VT[d["order_price_type"]]
            order.direction = DIRECTION_HBDM2VT[d["direction"]]
            order.offset = OFFSET_HBDM2VT[d["offset"]]
            order.status = statusMapReverse[d["status"]]
            order.orderTime = time
            order.gatewayName = self.gateway_name

            self.gateway.onOrder(order)

        self.gateway.writeLog(f"{request.extra}活动委托信息查询成功")

    def on_query_history_order(self,data,request):
        if self.check_error(data, "查询历史委托"):
            return
        for d in data["data"]["orders"]:
            timestamp = d["create_date"]
            dt = datetime.fromtimestamp(timestamp / 1000)
            time = dt.strftime("%H:%M:%S")

            orderid = d["order_id"]

            order = VtOrderData
            order.orderID = orderid
            order.symbol = d["contract_code"]
            order.exchange = EXCHANGE_HBDM
            order.price = d["price"]
            order.totalVolume = d["volume"]
            order.priceType = ORDERTYPE_HBDM2VT[d["order_price_type"]]
            order.direction = DIRECTION_HBDM2VT[d["direction"]]
            order.tradedVolume = d["trade_volume"]
            order.offset = OFFSET_HBDM2VT[d["offset"]]
            order.status = statusMapReverse[d["status"]]
            order.orderTime = time
            order.gatewayName = self.gateway_name,

            self.gateway.onOrder(order)

        self.gateway.writeLog(f"{request.extra}历史委托信息查询成功")

    def on_query_trade(self, data, request):
        """"""
        if self.check_error(data, "查询成交"):
            return

        for d in data["data"]["trades"]:
            dt = datetime.fromtimestamp(d["create_date"] / 1000)
            time = dt.strftime("%H:%M:%S")

            trade = VtTradeData()
            trade.orderID = d["order_id"]
            trade.tradeID = d["match_id"]
            trade.symbol = d["contract_code"]
            trade.exchange = EXCHANGE_HBDM
            trade.volume = d["trade_volume"]
            trade.price = d["trade_price"]
            trade.direction = DIRECTION_HBDM2VT[d["direction"]]
            trade.offset = OFFSET_HBDM2VT[d["offset"]]
            trade.tradeTime = time
            trade.gatewayName = self.gatewayName
            self.gateway.onTrade(trade)

        self.gateway.writeLog(f"{request.extra}成交信息查询成功")

    # ----------------------------------------------------------------------
    def onQueryContract(self, data, request):  # type: (dict, Request)->None
        """"""

        if self.check_error(data, "查询合约"):
            return

        for d in data['data']:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName

            contract.symbol = d["contract_code"]
            contract.exchange = EXCHANGE_HBDM
            contract.vtSymbol = '.'.join([contract.symbol, contract.exchange])

            contract.name = d["contract_code"]
            contract.priceTick = d["price_tick"]
            contract.size = int(d["contract_size"]),
            contract.productClass = PRODUCT_FUTURES

            self.gateway.onContract(contract)

            symbol_type_map[contract.symbol] = d['contract_type']

        self.queryOrder()
        self.queryTrade()

        # ----------------------------------------------------------------------

    def onSendOrder(self, data, request):  # type: (dict, Request)->None
        """"""
        order = request.extra

        if self.check_error(data, "委托"):
            order.status = STATUS_REJECTED
            self.gateway.onOrder(order)

    def on_send_order_failed(self, status_code, request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = STATUS_REJECTED
        self.gateway.onOrder(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.writeLog(msg)

    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = STATUS_REJECTED
        self.gateway.onOrder(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.onError(exception_type, exception_value, tb, request)


    def onCancelOrder(self, data, request):  # type: (dict, Request)->None
        """"""
        if self.check_error(data, "撤单"):
            self.gateway.writeLog(u'委托撤单成功：%s' % data)

    def on_cancel_order_failed(self, status_code, request):
        """
        Callback when canceling order failed on server.
        """
        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.writeLog(msg)

    def on_send_orders(self, data, request):
        """"""
        orders = request.extra

        errors = data.get("errors", None)
        if errors:
            for d in errors:
                ix = d["index"]
                code = d["err_code"]
                msg = d["err_msg"]

                order = orders[ix]
                order.status = STATUS_REJECTED
                self.gateway.onOrder(order)

                msg = f"批量委托失败，状态码：{code}，信息：{msg}"
                self.gateway.writeLog(msg)

    def on_send_orders_failed(self, status_code: str, request: Request):
        """
        Callback when sending order failed on server.
        """
        orders = request.extra

        for order in orders:
            order.status = STATUS_REJECTED
            self.gateway.onOrder(order)

        msg = f"批量委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.writeLog(msg)

    def on_send_orders_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        orders = request.extra

        for order in orders:
            order.status = STATUS_REJECTED
            self.gateway.onOrder(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.onError(exception_type, exception_value, tb, request)


    def check_error(self, data, func: str = ''):
        if data['status'] != 'error':
            return False
        error_code = data['err_code']
        error_msg = data['err_msg']

        self.gateway.writeLog('{0}请求出错，代码：{1}，信息：{2}'.format(func, error_code, error_msg))

        return True


########################################################################
class HbdmWebsocketApiBase(WebsocketClient):

    # ----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(HbdmWebsocketApiBase, self).__init__()

        self.gateway = gateway
        self.gatewayName = gateway.gatewayName

        self.apiKey = ''
        self.apiSecret = ''
        self.signHost = ''
        self.path = ''
        self.req_id = 0

    # ----------------------------------------------------------------------
    def connect(self, apiKey, apiSecret, url):
        """"""
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        host, path = _split_url(url)

        self.init(url)
        self.signHost = host
        self.path = path
        self.start()

    # ----------------------------------------------------------------------
    def login(self):

        self.req_id += 1
        params = {
            'op': 'auth',
            "type": "api",
            "cid": str(self.req_id),
        }
        params.update(
            createSignature(self.apiKey,
                            'GET',
                            self.signHost,
                            self.path,
                            self.apiSecret)
        )
        return self.sendPacket(params)

    # ----------------------------------------------------------------------
    def onLogin(self, packet):
        """"""
        pass

    # ----------------------------------------------------------------------
    @staticmethod
    def unpackData(data):
        return json.loads(zlib.decompress(data, 31))

        # ----------------------------------------------------------------------

    def onPacket(self, packet):
        """"""
        if 'ping' in packet:
            self.sendPacket({'pong': packet['ping']})
            return

        elif "op" in packet and packet["op"] == "ping":
            self.sendPacket({'op': 'pong', 'ts': packet['ts']})
            return

        elif 'err-msg' in packet:
            return self.onErrorMsg(packet)

        elif "op" in packet and packet["op"] == "auth":
            return self.onLogin(packet)
        else:

            self.onData(packet)

    # ----------------------------------------------------------------------
    def onData(self, packet):  # type: (dict)->None
        """"""
        print("data : {}".format(packet))

    # ----------------------------------------------------------------------
    def onErrorMsg(self, packet):  # type: (dict)->None
        """"""
        msg = packet['err-msg']
        if msg == u'invalid pong':
            return

        self.gateway.writeLog(packet['err-msg'])


########################################################################
class HbdmTradeWebsocketApi(HbdmWebsocketApiBase):

    # ----------------------------------------------------------------------
    def __init__(self, gateway):
        """"""
        super(HbdmTradeWebsocketApi, self).__init__(gateway)

        # self.accountDict = gateway.accountDict
        # self.orderDict = gateway.orderDict
        # self.orderLocalDict = gateway.orderLocalDict
        # self.localOrderDict = gateway.localOrderDict
    # ----------------------------------------------------------------------
    def connect(self, symbols, apiKey, apiSecret):


        self.symbols = symbols

        super(HbdmTradeWebsocketApi, self).connect(apiKey,
                                                    apiSecret,
                                                    WEBSOCKET_TRADE_HOST)

    # ----------------------------------------------------------------------
    def subscribeTopic(self):
        """"""
        # 订阅委托变动
        for symbol in self.symbols:
            symbol = symbol.split('_')[0]
            self.req_id += 1
            req = {
                "op": "sub",
                "cid": str(self.req_id),
                "topic": 'orders.%s' % symbol
            }
            self.sendPacket(req)

    # ----------------------------------------------------------------------
    def onConnected(self):
        """"""
        self.login()

    # ----------------------------------------------------------------------
    def onLogin(self, packet):
        """"""
        self.gateway.writeLog(u'交易Websocket服务器登录成功')

        self.subscribeTopic()

    # ----------------------------------------------------------------------
    def onData(self, packet):  # type: (dict)->None
        """"""
        op = packet.get('op', None)
        if op != 'notify':
            return

        topic = packet['topic']
        if 'orders' in topic:
            self.onOrder(packet['data'])

    # ----------------------------------------------------------------------
    def onOrder(self, data):

        dt = datetime.fromtimestamp(data["created_at"] / 1000)
        time = dt.strftime("%H:%M:%S")

        if data["client_order_id"]:
            orderID = data["client_order_id"]
        else:
            orderID = data["order_id"]


        order = VtOrderData()
        order.symbol = data["contract_code"]
        order.exchange = EXCHANGE_HBDM
        order.orderID = orderID
        order.priceType = ORDERTYPE_HBDM2VT[data["order_price_type"]]
        order.direction = DIRECTION_HBDM2VT[data["direction"]]
        order.offset = OFFSET_HBDM2VT[data["offset"]]
        order.price = data["price"]
        order.volume = data['volume']
        order.tradedVolume = data["trade_volume"]
        order.status = statusMapReverse[data["status"]]
        order.orderTime = time
        self.gateway.onOrder(order)

        trades = data['trade']
        if not trades:
            return
        for d in trades:
            dt = datetime.fromtimestamp(d["created_at"] / 1000)
            time = dt.strftime("%H:%M:%S")

            trade = VtTradeData()
            trade.gatewayName = self.gatewayName
            trade.symbol = order.symbol
            trade.vtSymbol = '.'.join([trade.symbol, trade.exchange])
            trade.exchange = EXCHANGE_HBDM
            trade.orderID = order.orderID
            trade.tradeID = str(data['trade-id'])
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = data['trade_price']
            trade.volume = data['trade_volume']
            trade.tradeTime = time
            self.gateway.onTrade(trade)


########################################################################
class HbdmMarketWebsocketApi(HbdmWebsocketApiBase):

    # ----------------------------------------------------------------------
    def __init__(self, gateway):
        """"""
        super(HbdmMarketWebsocketApi, self).__init__(gateway)

        self.tickDict = {}

    # ----------------------------------------------------------------------
    def connect(self, symbols, apiKey, apiSecret):
        """"""
        self.symbols = symbols

        super(HbdmMarketWebsocketApi, self).connect(apiKey,
                                                     apiSecret,
                                                     WEBSOCKET_MARKET_HOST)

    # ----------------------------------------------------------------------
    def onConnected(self):
        """"""
        self.subscribeTopic()

    # ----------------------------------------------------------------------
    def subscribeTopic(self):  # type:()->None
        """
        """
        for symbol in self.symbols:
            # 创建Tick对象
            tick = VtTickData()
            tick.gatewayName = self.gatewayName
            tick.symbol = symbol
            tick.exchange = EXCHANGE_HBDM
            tick.vtSymbol = '.'.join([tick.symbol, tick.exchange])
            self.tickDict[symbol] = tick

            # 订阅深度和成交
            self.req_id += 1
            req = {
                "sub": "market.%s.depth.step0" % symbol,
                "id": str(self.req_id)
            }
            self.sendPacket(req)

            self.req_id += 1
            req = {
                "sub": "market.%s.detail" % symbol,
                "id": str(self.req_id)
            }
            self.sendPacket(req)

    # ----------------------------------------------------------------------
    def onData(self, packet):  # type: (dict)->None
        """"""
        channel = packet.get("ch", None)
        if channel:
            if 'depth.step' in channel:
                self.onMarketDepth(packet)
            elif 'detail' in channel:
                self.onMarketDetail(packet)
        elif 'err-code' in packet:
            self.gateway.writeLog(u'错误代码：%s, 信息：%s' % (packet['err-code'], packet['err-msg']))

    # ----------------------------------------------------------------------
    def onMarketDepth(self, data):
        """行情深度推送 """
        symbol = data['ch'].split('.')[1]

        tick = self.tickDict.get(symbol, None)
        if not tick:
            return

        tick.datetime = datetime.fromtimestamp(data['ts'] / 1000)
        tick.date = tick.datetime.strftime('%Y%m%d')
        tick.time = tick.datetime.strftime('%H:%M:%S.%f')

        bids = data['tick']['bids']
        for n in range(5):
            l = bids[n]
            tick.__setattr__('bidPrice' + str(n + 1), float(l[0]))
            tick.__setattr__('bidVolume' + str(n + 1), float(l[1]))

        asks = data['tick']['asks']
        for n in range(5):
            l = asks[n]
            tick.__setattr__('askPrice' + str(n + 1), float(l[0]))
            tick.__setattr__('askVolume' + str(n + 1), float(l[1]))

        if tick.lastPrice:
            newtick = copy(tick)
            self.gateway.onTick(newtick)

    # ----------------------------------------------------------------------
    def onMarketDetail(self, data):
        """市场细节推送"""
        symbol = data['ch'].split('.')[1]

        tick = self.tickDict.get(symbol, None)
        if not tick:
            return

        tick.datetime = datetime.fromtimestamp(data['ts'] / 1000)
        tick.date = tick.datetime.strftime('%Y%m%d')
        tick.time = tick.datetime.strftime('%H:%M:%S.%f')

        t = data['tick']
        tick.openPrice = float(t['open'])
        tick.highPrice = float(t['high'])
        tick.lowPrice = float(t['low'])
        tick.lastPrice = float(t['close'])
        tick.volume = float(t['vol'])
        tick.preClosePrice = float(tick.openPrice)

        if tick.bidPrice1:
            newtick = copy(tick)
            self.gateway.onTick(newtick)

