#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "TradeAPI Declaration and Implementation"

from abc import ABCMeta, abstractmethod
import pandas as pd
import logging as log
import os,sys
import ibapi
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

#---------------------------------------------
# abstract class implementation
#---------------------------------------------
class TradeAPI(object):
    __metaclass__ = ABCMeta
    _name_ = "abstract"
    cwd = os.path.dirname(os.path.realpath(__file__))
    dataPath = cwd + "/../../data"
    def __init__(self, args=None):
        self.args = args
    @abstractmethod
    def exeOrders(self):
        raise NotImplementedError("")

# https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/
class IBAPI(TradeAPI,EWrapper,EClient):
    name = "IBAPI"
    def __init__(self, **kwargs):
        EClient.__init__(self, self)
        super(IBAPI, self).__init__(**kwargs)
    def exeOrders(self):
        log.warn('exeOrders is called')

