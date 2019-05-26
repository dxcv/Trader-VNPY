from __future__ import absolute_import
from vnpy.trader import vtConstant
from .hbdmGateway import HbdmGateway

gatewayClass = HbdmGateway
gatewayName = 'HBDM'
gatewayDisplayName = u'火币合约'
gatewayType = vtConstant.GATEWAYTYPE_BTC
gatewayQryEnabled = False