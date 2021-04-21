#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "Data API Implementation"

import sys, os, traceback
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import qpython
from qpython import qconnection
from qpython.qtype import *
import datetime,glob,pickle
import logging as log
import pandas_datareader as pdr

#---------------------------------------------
# abstract Signal class 
#
#---------------------------------------------
class SignalAPI(object):
    __metaclass__ = ABCMeta
    _name_ = "abstractData"
    def __init__(self, args=None):
        self.args = args
        self.Sym = args.Sym
        self.dates = args.dates
        self.sDate = args.sDate = self.dates[0].strftime('%Y-%m-%d')
        self.eDate = args.eDate = self.dates[-1].strftime('%Y-%m-%d')
        if hasattr(args,'view') and args.view: self.file = args.view
        else: self.file = None
        self.dataPath = "data"
        self.docPath = "doc"
        if not os.path.isdir(self.dataPath):
            if os.path.isdir("../data"):
                self.dataPath = "../data"
                self.docPath = "../doc"
            else: self.dataPath=self.docPath="."

    @abstractmethod
    def DF(self, args=None):
        raise NotImplementedError("")

#---------------------------------------------
# Data Scraper implementation
#
#---------------------------------------------
class DataScrape(SignalAPI):
    def __init__(self, **kwargs):
        super(DataScrape, self).__init__(**kwargs)
        self.API_KEY='a5fda378f194b64c8733a4cb60d816a9b6abdd2f'
    def DF(self, args=None):
        if not args: args = self.args
        df1 = pdr.get_data_tiingo(self.Sym, start=self.sDate, end=self.eDate, api_key=self.API_KEY)
        df1['close'] = df1['adjClose']
        df1['open'] = df1['adjOpen']
        self._df = df1 = df1.reset_index()
        rets = (df1.close - df1.close.shift(1))/df1.close.shift(1)
        self._df['Ret'] = rets
        if 'datetime64[ns' in str(self._df['date'].dtype): self._df['date'] = self._df['date'].dt.date
        if 'sym' not in self._df.columns and 'symbol' in self._df.columns: self._df['sym'] = self._df['symbol']
        self._df.to_pickle(self.dataPath+'/store/{}_{}_{}.pkl'.format(self.Sym,self.sDate,self.eDate))
        return self._df

#---------------------------------------------
# Data Store implementation
#
#---------------------------------------------
class DataStore(SignalAPI):
    def __init__(self, **kwargs):
        super(DataStore, self).__init__(**kwargs)
    def DF(self, args=None):
        if not args: args = self.args
        self.Sym = args.Sym
        self.dates = args.dates
        self.sDate = args.sDate = self.dates[0].strftime('%Y.%m.%d')
        self.eDate = args.eDate = self.dates[-1].strftime('%Y.%m.%d')
        self.columns = ['date','sym','open','high','low','close','volume','exch','Ret','alpha','ordId']
        self._df = pd.DataFrame(self.columns)
        with open(args.file, 'rb') as pfh:
            self._df = pd.read_pickle(pfh)
            self._df = self._df.reset_index()
        # select slice between start and end dates
        df1 = self._df = self._df.loc[(args.dates[0]<=self._df['date']) & (self._df['date']<=args.dates[-1])].reset_index()
        rets = (df1.close - df1.close.shift(1))/df1.close.shift(1)
        self._df['Ret'] = rets
        if 'sym' not in self._df.columns and 'symbol' in self._df.columns: self._df['sym'] = self._df['symbol']
        return self._df

class KDBData(SignalAPI):
    def __init__(self, **kwargs):
        try:
            self.q = qpython.qconnection.QConnection(host = 'localhost', port = 5001, pandas = True)
            self.q.open()
        except:
            self.error = "KDB instance is down, bring it up and try again"
            log.warn(self.error)
            self.q = None
            if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
        super(KDBData, self).__init__(**kwargs)
    def exec_q(self,qquery=None):
        if not qquery: qquery = self.qquery
        if not self.q:
            log.error("ERROR: cannot execute q query, make sure kdb is running")
            raise Exception(self.error)
        try:
            log.warn('executing query:{}'.format(qquery))
            return self.q.sendSync(qquery)
        except:
            self.error = "ERROR in q query"
            log.error(self.error)
            if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
            return []
    def calcAlpha(self):
        self.qquery = 'getDF[{};`{}]'.format("({};{})".format(self.sDate,self.eDate),self.Sym)
        self._df=self.exec_q()
    def DF(self, args=None):
        if not args: args = self.args
        self.Sym = args.Sym
        self.dates = args.dates
        self.sDate = self.dates[0].strftime('%Y.%m.%d')
        self.eDate = self.dates[-1].strftime('%Y.%m.%d')
        try:
            self.calcAlpha()
        except:
            self.error = "ERROR in KDB"
            log.error(self.error)
            sys.exit(-2)
        return self._df
