#!/usr/bin/env python
__author__ = "Arturas Vaitaitis"
__version__ = "1.1"
__description__ = "Optimal Stop Loss Generator"
import sys, os, traceback
import logging as log
import pandas as pd
import numpy as np
import re,socket,datetime
from lib.Strategy import * # generate trades from signal
import tabulate
import glob
import warnings
#------------------------------------------------------------------------------------
# Portfolio dynamically generated from configured strategies
#
# Optimal Stop Loss order creation with near-real time backtesting
#
# heavy processing and stateless alpha is calculated in Strategy object or in kdb and wrapped in Signal object
# state manipulation and portfolio management is done in PNL object 
#------------------------------------------------------------------------------------
class PNLPortfolio:
    dataPath = "../data"
    docPath = "../doc"
    pnlFile = dataPath + "/PNL"
    pnlFileCsv = pnlFile + ".csv"

    def __init__(self, args):
        self.args = args
        if hasattr(args,'view') and args.view: self.file = args.view
        else: self.file = None
        self.Sym = args.Sym
        self._df = None
        self.isScrapeData = True
        self.isKdbData = False
        self.dates = args.dates
        self.horizon = (self.dates[-1] - self.dates[0]).days
        self.sDate = self.dates[0].strftime('%Y.%m.%d')
        self.eDate = self.dates[-1].strftime('%Y.%m.%d')
        if hasattr(args,'out') and args.out:
            if self.Sym and self.dates and self.out!=PNLPortfolio.pnlFile:
                args.out = '{}/sl_{}_{}_{}.csv'.format(self.signal.dataPath,self.Sym,self.sDate,self.eDate)
        if not os.path.isdir(PNLPortfolio.dataPath):
            if os.path.isdir("data"):
                PNLPortfolio.dataPath = "data"
                PNLPortfolio.docPath = "doc"
            else: PNLPortfolio.dataPath=PNLPortfolio.docPath="."
        self._dataSet = []
        if hasattr(args,'stratPath') and args.stratPath: self.stratPath = args.stratPath
        else: args.stratPath = self.stratPath = PNLPortfolio.dataPath + '/strats'
        self.createPortfolio()

    def SignalAPIFactory(self):
        args = self.args
        if self.isKdbData: # invoke kdb
            return KDBData(args=args)
        dataFiles = glob.glob(PNLPortfolio.dataPath+'/store/'+args.Sym+'*.pkl')
        if dataFiles and len(dataFiles)>0:
            log.debug('Found {cnt} data files for {sym}'.format(cnt=len(dataFiles),sym=args.Sym))
            for f1 in dataFiles:
                try:
                    s1,sd1,ed1 = f1.split('/store/')[-1].split('.pkl')[0].split('_')
                    sFile = (datetime.datetime.strptime(sd1,'%Y-%m-%d')).date()
                    eFile = (datetime.datetime.strptime(ed1,'%Y-%m-%d')).date()
                    #if sFile<self.args.dates[-1] or eFile>self.args.dates[0]:
                    # start date is within file timeframe, though end date is just greater than start of file
                    if sFile<=self.args.dates[0] and sFile<self.args.dates[-1] and self.args.dates[0]<=eFile:
                        args.file = f1
                        log.info('located data {file} for {sym} and {dates}'.format(file=f1,sym=args.Sym,dates=self.args.dates))
                        self.isScrapeData = False
                        return DataStore(args=args)
                except:
                    if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
                    continue
        if self.isScrapeData:
            log.warn('data not found, attempting to generate file for {sym}'.format(sym=args.Sym))
            # scrape data file
            return DataScrape(args=args)

    def setDF(self):
        args = self.args
        try:
            self.dataAPI = self.SignalAPIFactory()
            self._df=self.dataAPI.DF(args)
        except:
            errStr='cannot get data frame, kdb is down, restart and try again'
            log.error(errStr)
            if log.DEBUG>=log.getLogger().getEffectiveLevel(): traceback.print_exc()
            raise Exception(errStr)
        args._df = self._df
        self.strats = StrategyFactory.create(self.stratPath, args=args)
        self.stratDict = {s1.getName():s1 for s1 in self.strats}

    def getDF(self): # API
        if not isinstance(self._df, pd.DataFrame) or self._df.empty: self.setDF()
        df = self._df
        if 'alpha' in df: df = df.drop('alpha', axis=1)
        return df

    # find most optimal strategy and return data for it
    def createPortfolio(self):
        args = self.args
        fss,dfRDCache = [],None
        if hasattr(self.args,'nodates') and self.args.nodates:
            fss = glob.glob(PNLPortfolio.dataPath+'/cache/p{}DF_*.pkl'.format(self.Sym))
        else:
            fss = glob.glob(PNLPortfolio.dataPath+'/cache/p{}DF_{}-{}.pkl'.format(self.Sym,self.sDate,self.eDate))
        if fss and len(fss)>0: dfRDCache = fss[0]
        if self.file and not self.file=="NA": dfWRCache = dfRDCache = self.file
        if not (hasattr(self.args,'force') and self.args.force) and dfRDCache and os.path.isfile(dfRDCache):
            log.warn('Found cached result {cache} for {sym}'.format(cache=dfRDCache,sym=args.Sym))
            self._portDF = pd.read_pickle(dfRDCache)
        else:
            self._df = self.getDF()
            strats = self.strats 
            dfd = []
            for strat in strats:
                pnl = strat.pnl
                sharpe = strat.sharpe
                trades = strat.getPos()
                trades['stratName'] = strat.getName()
                cnt = len(trades)
                if cnt<2: rank=0.0
                elif 'Custom' in strat.getType(): rank = 100
                else: rank = pnl # *math.sqrt(cnt)
                dfd.append({'name': strat.getName(), 'pnl': pnl, 'rank': rank, 'sym': self.Sym, 'sharpe': sharpe, \
                    'days': self.horizon, 'trades': trades, 'cnt': cnt})
            self._portDF = pd.DataFrame(dfd)
            self._portDF = self._portDF.sort_values(by=['rank'],ascending=False)
        if isinstance(self._portDF, pd.DataFrame) and not self._portDF.empty:
            dfWRCache = PNLPortfolio.dataPath+'/cache/p{}DF_{}-{}.pkl'.format(self.Sym,self.sDate,self.eDate)
            try:self._portDF.to_pickle(dfWRCache)
            except:traceback.print_exc()
        if log.INFO>=log.getLogger().getEffectiveLevel():
            print(tabulate.tabulate(self.getPortDF(), headers='keys', tablefmt='psql'))
        if log.DEBUG>=log.getLogger().getEffectiveLevel():
            for ii,rr in self._portDF.iterrows():
                print(rr)
            print(self.getTrades())

    def getPNL(self):
        pnl = 0
        for ii,rr in self._portDF.iterrows():
            pnl += rr['pnl']
        return pnl

    def getData1(self, ii):
        return self._portDF.iloc[ii-1]

    # API 
    def getTrades(self):
        Trades = []
        for ii,rr in self._portDF.iterrows():
            Trades.append(rr['trades'])
        tradesDF = pd.concat(Trades).drop_duplicates().reset_index(drop=True)
        return tradesDF

    def getPortDF(self): # API 
        return self._portDF.drop(['trades'], axis=1)

    def getStrategy(self, name=None): # API 
        if name: return self.stratDict[name]
        else: return self.strats

    def getPortfolioDataPath(self):
        return PNLPortfolio.dataPath


class PNLPortfolios:
    def __init__(self, args):
        self.args = args
        if isinstance(args.Syms, list): self.Syms = args.Syms
        else: self.Syms = args.Syms.split(',')
        self.portfolios = {}
        for Sym in self.Syms:
            args.Sym = Sym
            self.portfolios[Sym] = PNLPortfolio(args)

    def getDF(self, Sym=None):
        dfs = []
        if Sym: return self.portfolios[Sym].getDF()
        else:
            for Sym in self.Syms:
                dfs.append(self.portfolios[Sym].getDF())
            return pd.concat(dfs).drop_duplicates().reset_index(drop=True)

    def getPortDF(self, Sym=None):
        dfs = []
        if Sym: return self.portfolios[Sym].getPortDF().drop(['rank'], axis=1)
        else:
            for Sym in self.Syms:
                dfs.append(self.portfolios[Sym].getPortDF())
            pdf = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
            pdf = pdf.sort_values(by=['rank'],ascending=False).drop(['rank'], axis=1)
            return pdf

    def getTrades(self, Sym=None):
        dfs = []
        if Sym: return self.portfolios[Sym].getTrades()
        else:
            for Sym in self.Syms:
                dfs.append(self.portfolios[Sym].getTrades())
            return pd.concat(dfs).drop_duplicates().reset_index(drop=True)

    def getStrategy(self, Sym=None, name=None):
        if Sym: return self.portfolios[Sym].getStrategy()
        else:
            Sym=list(self.portfolios.keys())[0]
            return self.portfolios[Sym].getStrategy(name=name)

    def setStrategy(self, account):
        args = self.args
        for Sym in self.Syms:
            args.Sym = Sym
            args.saveStrat = True
            args.account = account
            self.portfolios[Sym] = PNLPortfolio(args)
