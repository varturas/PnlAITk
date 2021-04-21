#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "Alpha Implementations"

import sys, os, traceback, errno, inspect
from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import datetime,glob,pickle
import logging as log
import talib as ta

class AlphaFactory():
    @staticmethod
    def create(klasStr, **kwargs):
        try:
            modClss = inspect.getmembers(sys.modules[__name__], inspect.isclass)
            if not klasStr in [ee[0] for ee in modClss]: klasStr = 'CustomAlpha'
            kls = globals()[klasStr]
            return kls(**kwargs)
        except:
            traceback.print_exc()
            return None

#---------------------------------------------
# Alpha Calcs implementations
#---------------------------------------------
class BaseAlpha(object):
    __metaclass__ = ABCMeta
    _name_ = "BaseAlpha"
    def __init__(self, args=None):
        self.alpha = np.nan
        self.args = args
    @abstractmethod
    def calc(self, df):
        raise NotImplementedError("")
    def getAlpha(self):
        return self.alpha

#---------------------------------------------
class CustomAlpha(BaseAlpha):
    _name_ = "CustomAlpha"
    def __init__(self, **kwargs):
        super(CustomAlpha, self).__init__(**kwargs)
    def calc(self, df):
        df['alpha'] = self.alpha = np.nan
        return df

class RallyAlpha(BaseAlpha):
    _name_ = "RallyAlpha"
    def __init__(self, **kwargs):
        super(RallyAlpha, self).__init__(**kwargs)
        self.ndays = 5
        self.fact = 2.0
        self.maxRet = 0.05
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        Ret1 = df['Ret']
        dev1 = (Ret1.shift(1)-Ret1.shift(1).rolling(self.ndays).mean()).rolling(self.ndays).std()
        a1 = Ret1 < -0.5*self.fact*dev1 # abrupt relative drop in Ret
        a2 = Ret1 < -1.*self.maxRet # abrupt absolute drop
        RetN = (df.close.shift(1) - df.close.shift(self.ndays))/df.close.shift(self.ndays)
        b1 = RetN > 0.25*self.fact*dev1 # following abrupt relative rally
        b2 = RetN > 0.5*self.maxRet # or abrupt absolute rally
        df['alpha'] = self.alpha = np.where((a1&a2)&(b1|b2),df['close'].shift(1)*(1.-0.25*self.fact*dev1),np.nan)
        #df['alpha'] = np.where(a2&b2,df['close'].shift(1)*(1.-0.5*self.fact*dev1),np.nan)
        self._df = df
        return df

class AbruptDrop(BaseAlpha):
    _name_ = "AbruptDrop"
    def __init__(self, **kwargs):
        super(AbruptDrop, self).__init__(**kwargs)
        self.ndays = 5
        self.fact = 2.0
        self.maxRet = 0.10
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        Ret1 = df['Ret']
        dev1 = (Ret1.shift(1)-Ret1.shift(1).rolling(self.ndays).mean()).rolling(self.ndays).std()
        a1 = Ret1 < -1.*self.maxRet # abrupt absolute drop
        RetN = (df.close.shift(1) - df.close.shift(self.ndays))/df.close.shift(self.ndays)
        df['alpha'] = self.alpha = np.where(a1,df['close'].shift(1)*(1.-0.25*self.fact*dev1),np.nan)
        return df

class StopLoss(BaseAlpha):
    _name_ = "StopLoss"
    def __init__(self, **kwargs):
        super(StopLoss, self).__init__(**kwargs)
        self.ndays = 5
        self.maxRet = -0.02
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        Ret1 = df.Ret
        negRet = df.Ret[df['Ret']<0]
        self.maxRet = 2*negRet.mean()
        a1 = Ret1 < self.maxRet # abrupt absolute drop
        df['alpha'] = self.alpha = np.where(a1,df.close,np.nan)
        return df

class RSI(BaseAlpha):
    _name_ = "RSI"
    def __init__(self, **kwargs):
        super(RSI, self).__init__(**kwargs)
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        df['alpha'] = self.alpha = ta.RSI(df['close'],14)
        return df

class BollingerBands(BaseAlpha):
    _name_ = "BollingerBands"
    def __init__(self, **kwargs):
        super(BollingerBands, self).__init__(**kwargs)
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        df['upper'],df['middle'],df['lower'] = upper, middle, lower = ta.BBANDS(df['close'], timeperiod=14, nbdevup=2, nbdevdn=2, matype=ta.MA_Type.T3)
        df['alpha'] = self.alpha = middle
        return df

class MACD(BaseAlpha):
    _name_ = "MACD"
    def __init__(self, **kwargs):
        super(MACD, self).__init__(**kwargs)
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        df['macd'],df['signal'],df['macdhist'] = macd,signal,macdhist = ta.MACD(df['close'], 12, 26, 9)
        df['alpha'] = self.alpha = macd
        return df

class WMA(BaseAlpha):
    _name_ = "WMA"
    def __init__(self, **kwargs):
        super(WMA, self).__init__(**kwargs)
    def calc(self, df):
        if not isinstance(df, pd.DataFrame) or df.empty: raise Exception("null dataframe")
        df['alpha'] = self.alpha = ta.WMA(df['close'], 50)
        return df
