#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "Strategies Implementation"

import sys, os, traceback, errno, time
from abc import ABCMeta, abstractmethod
from lib.Alpha import *
from lib.DataAPI import *
from multiprocessing import *
import pandas as pd
import numpy as np
import datetime,glob,pickle
import logging as log
import configparser

#---------------------------------------------
# abstract backtest implementation
#
#---------------------------------------------
class BaseStrategy(object):
    __metaclass__ = ABCMeta
    stratPath = "../data/strats/"
    def __init__(self, name, classtype, args=None):
        import builtins
        self._type = classtype
        self.args = args
        self._df = args._df
        self.pnl = None
        self.alphaVals = np.nan
        self._pos = pd.DataFrame(columns=['date','sym','price','quantity'])
        self._summary = pd.DataFrame(columns=['name','PNL','performance'])
        self.cfg = configparser.ConfigParser()
        self.fee = 0.0000221
        self.cost = 0.005
        #self._name = self.__class__.__name__
        self._name = name
        cfgset = None
        if not os.path.isdir(self.stratPath): BaseStrategy.stratPath = "data/strats/"
        try:
            cfgFile = BaseStrategy.stratPath + self._name + '.cfg'
            if os.path.isfile(cfgFile):
                cfgset = self.cfg.read(cfgFile)
                if cfgset is None or len(cfgset)<1: raise Exception('empty configuration')
            elif args.backtest and args.backtest != "NA":
                self.cfg.read_string(args.backtest.replace("\\n", "\n").replace(",", "\n"))
                if hasattr(args,'account') and args.account and hasattr(args,'saveStrat') and args.saveStrat:
                    cfgPath = BaseStrategy.stratPath + self._name + '.cfg'
                    with builtins.open(cfgPath, 'w') as cfgh:
                        self.cfg.write(cfgh) # save strategy
            else: raise Exception('unknown config type ' + self._name)
        except: traceback.print_exc()
        cfg1 = self.cfg['STRATEGY']
        #create alpha
        if 'alphaName' in cfg1: self.alphaName = cfg1['alphaName']
        else:
            log.warn('config missing alphaName, default to {}'.format(self._name))
            self.alphaName = self._name
        self.alpha = AlphaFactory.create(self.alphaName, args=args)
        if 'mode' in cfg1: self.mode = cfg1['mode']
        else: self.mode = "DollarNeutral"
        if 'days' in cfg1: self.alpha.args.days = self.days = int(cfg1['days'])
        else: self.alpha.args.days = self.days = 120
        if 'buyAction' in cfg1: self.buyAction = cfg1['buyAction']
        else: self.buyAction = "cover&long"
        if 'sellAction' in cfg1: self.sellAction = cfg1['sellAction']
        else: self.sellAction = "close&short"
        if 'maxQuantity' in cfg1: self.maxQuantity = cfg1['maxQuantity']
        else: self.maxQuantity = 1
        try:
            self.run()
            self.finalize()
        except: traceback.print_exc()
    def getPos(self):
        return self._pos
    def getSummary(self):
        return self._summary
    def getName(self):
        return self._name
    def getType(self):
        return self._type
    def getAlpha(self):
        return self.alphaVals
    def run(self):
        if self._df is None: raise Exception('empty dataframe')
        cfg1 = self.cfg['STRATEGY']
        # calculate alpha
        self.dfAlpha = self.alpha.calc(self._df)
        self.alphaVals = self.alpha.getAlpha()
        days,totQuantity = 0,0
        self.Pos = []
        #upper,middle,lower=np.nan,np.nan,np.nan
        lastAlpha,lastClose,lastOpen,lastHigh,lastLow,lastPrice=[np.nan]*6
        for ii,rr in self.dfAlpha.iterrows():
            globals().update(rr);locals().update(rr)
            acts = []
            Quantity = 0
            Price = np.nan
            if 'days' in cfg1: days = int(cfg1['days'])
            positionExpired = days>0 and ii>=days and self._df.iloc[ii-days]['alpha']!=0 and abs(totQuantity)>0
            LastDay = GoodTillCancel = ii==len(self._df.index)-1
            FirstDay = ii==0
            if 'buyCondition' in cfg1 and eval(cfg1['buyCondition']):
                qty1 = 1.0*int(cfg1['buyQuantity'])
                # allow the trade is max quantity is unbound or if totQuantity post trade does not exceed the max
                if self.maxQuantity < 0 or abs(qty1 + totQuantity) <= self.maxQuantity:
                    Quantity = qty1
                    if 'buyPrice' in cfg1: Price = eval(cfg1['buyPrice'])
                    if 'cover' in self.buyAction and totQuantity<0: # completely reverse position
                        Quantity += (-totQuantity)  # by adding negative of negative total quantity
                        acts.append('cover')
                    else: acts.append('buy')
            if 'sellCondition' in cfg1 and eval(cfg1['sellCondition']):
                qty1 = -1.0*int(cfg1['sellQuantity'])
                if self.maxQuantity < 0 or abs(qty1 + totQuantity) <= self.maxQuantity: # allow the trade
                    Quantity = qty1
                    if 'sellPrice' in cfg1: Price = eval(cfg1['sellPrice'])
                    if 'close' in self.sellAction and totQuantity>0: # completely reverse position
                        Quantity -= totQuantity 
                        acts.append('close')
                    else: acts.append('sell')
            if 'DollarNeutral' in self.mode and LastDay and abs(totQuantity+Quantity)>0:
                Quantity += -1.0*(totQuantity+Quantity)
                Price = close
                acts.append('force_close')
            action = ''
            if abs(Quantity)>0:
                totQuantity += Quantity
                if acts: action = ','.join(acts)
                self.Pos.append([date,sym,Price,Quantity,action])
            lastAlpha,lastClose,lastOpen,lastHigh,lastLow,lastPrice=alpha,close,open,high,low,Price
    def finalize(self):
        self._pos = pd.DataFrame(self.Pos,columns=['date','sym','price','quantity','action'])
        self._df = self._df.merge(self._pos, on=['date','sym'], how='left')
        dailyPnl = ((self._df['close']-self._df['price'].ffill())*self._df['quantity'].ffill()).cumsum()
        self._df['ret'] = dailyRet = dailyPnl.pct_change()
        dailyStd = dailyRet[~np.isnan(dailyRet)&~np.isinf(dailyRet)].std()
        if log.DEBUG>=log.getLogger().getEffectiveLevel(): print(self._pos)
        cfg1 = self.cfg['STRATEGY']
        pnl = (self.fee-1.0)*(self._pos.price*self._pos.quantity).sum() - self.cost*len(self._pos)
        if len(self._pos)>0: cap = abs(self._pos['price'][0]*self._pos['quantity'][0])
        else: cap = 0
        if cap>0: self.pnl = pnl = pnl / cap
        else: self.pnl = pnl = 0
        if len(self._pos)>2 and dailyStd>0.0: self.sharpe = np.sqrt(252) * pnl / dailyStd
        else: self.sharpe = np.nan
        self._summary.loc[0]=[self._name,pnl,pnl]
        log.debug('P&L: {}'.format(pnl))

class BaseClass(object):
    def __init__(self, classtype):
        self._type = classtype

def ClassFactory(name, Type, BaseClass=BaseClass):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        BaseClass.__init__(self, name, Type, **kwargs)
    newclass = type(Type, (BaseClass,),{"__init__": __init__})
    return newclass

class StrategyFactory():
    @staticmethod
    def create(stratPath, **kwargs):
        klss = []
        for ff in os.listdir(stratPath):
            if ff.endswith(".cfg"):
                klasStr = ff[:-4]
                if '_' in ff:
                    if hasattr(kwargs['args'],'account') and kwargs['args'].account:
                        acc = kwargs['args'].account
                        if acc != ff.split('_')[-1]:
                            log.warn('config {} does not belong to account {}, skipping'.format(ff,acc))
                            continue
                    else:
                        log.warn('skip account config {} without account info'.format(ff))
                        continue
                try:
                    #kls = globals()[klasStr]
                    kls = ClassFactory(klasStr, klasStr, BaseClass=BaseStrategy)
                    kls1 = kls(**kwargs)
                    cfg1 = kls1.cfg['STRATEGY']
                    for var1 in ['alphaName','buyCondition','buyPrice','buyQuantity','sellCondition','sellPrice','sellQuantity']:
                        if var1 in cfg1: setattr(kls, var1, cfg1[var1])
                        else: setattr(kls, var1, None)
                    klss.append(kls1)
                except:
                    traceback.print_exc()
                    log.error('Count not construct {} class'.format(klasStr))
                    continue
        try:
            if kwargs['args'].backtest and kwargs['args'].backtest!="NA":
                cfg = configparser.ConfigParser()
                cfg.read_string(kwargs['args'].backtest.replace("\\n", "\n").replace(",", "\n"))
                alphaName = cfg['STRATEGY']['alphaName']
                if hasattr(kwargs['args'],'account') and kwargs['args'].account: stratName = '***' + alphaName + '_' + kwargs['args'].account
                else: stratName = "Custom"
                kls = ClassFactory(stratName, alphaName, BaseClass=BaseStrategy)
                klss.append(kls(**kwargs))
        except:
            traceback.print_exc()
        return klss
