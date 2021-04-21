#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "BDD Strategies"

import sys, os, traceback, errno, time
from abc import ABCMeta, abstractmethod
from multiprocessing import *
import pandas as pd
import numpy as np
import datetime,glob,pickle
import logging as log
from lib.TradeAPI import IBAPI
import threading

#---------------------------------------------
# abstract class implementation
# use State Design Pattern
# utilized BDD behave framework
#---------------------------------------------
class BDDStrategy(object):
    __metaclass__ = ABCMeta
    _name_ = "abstract"
    cwd = os.path.dirname(os.path.realpath(__file__))
    dataPath = cwd + "/../../data"
    def __init__(self, args=None):
        self.args = args
    @abstractmethod
    def genOrders(self):
        raise NotImplementedError("")
    @abstractmethod
    def exeOrders(self):
        raise NotImplementedError("")

class StopLoss(BDDStrategy):
    name = "StopLoss"
    config = BDDStrategy.dataPath + "/features/stopLoss.feature"
    def __init__(self, **kwargs):
        super(StopLoss, self).__init__(**kwargs)

    def genOrders(self):
        log.warn('getOrders is called')
        from behave.__main__ import main as behave_main
        from behave.configuration import ConfigError
        try:
            behave_main([StopLoss.config])
        except ConfigError:
            print("Config Error")

    def run_loop(self):
        self.ib1.run()
    
    def exeOrders(self):
        log.warn('exeOrders is called')
        fname = StopLoss.config + '.pkl'
        if os.path.isfile(fname):
            dfO = pd.read_pickle(fname)
            print('Received orders DF:\n',dfO)
            args=self.args
            args.dfO = dfO
            self.ib1 = IBAPI(args=args)
            self.ib1.connect('127.0.0.1', 7496, 123)
            self.ib1.exeOrders()
            api_thread = threading.Thread(target=self.run_loop, daemon=True)
            api_thread.start()
            time.sleep(1)
            self.ib1.disconnect()

