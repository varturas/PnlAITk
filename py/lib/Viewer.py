import logging as log
import pandas as pd
import sys,signal,traceback
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mds
#import seaborn as sns
import warnings

warnings.filterwarnings("ignore")
#warnings.simplefilter(action='ignore', category=FutureWarning)
log.getLogger().setLevel(log.WARN)

class PNLViewer:
    def __init__(self, args):
        self.args = args
        self._pos = 0
        self.offset = 120
        self.nrows = 1
        self.Sym=self.args.Sym
        self.dates = self.args.dates
        #sns.set_style("darkgrid")
        self.portX = portX = args.portX
        self._df = portX.getDF()
        self.alpha = portX.getStrategy()[0].getAlpha()
        self._df['alpha'] = self.alpha
        self.trades = portX.getTrades().set_index('date')
        self.pnl = portX.getPNL()

    def on_event(self, e):
        log.debug("got a key event!")
        if log.DEBUG<=log.getLogger().getEffectiveLevel():
            print(e); sys.stdout.flush()
        if e.button:
            if e.button == 3:
                nLen = len(self._df.index)
                if self._pos<nLen-1: self._pos += self.offset
                else: self._pos = nLen-1
                log.debug("step forward to pos:%d" % (self._pos))
            elif e.button == 1 and e.dblclick:
                if self._pos>=1: self._pos -= self.offset
                else: self._pos=0
                log.debug("step back to pos:%d" % (self._pos))
            else:
                return
        else:
            return
        self.viewAt(self._pos)

    def viewAt(self, pos=None):
        if not pos: pos=self._pos
        if pos>=len(self._df.index): pos=len(self._df.index)-self.nrows-1 
        df1=self._df.sort_values('date').iloc[pos:pos+self.offset]
        tdf1=self.trades.reset_index()
        tdf1 = tdf1.loc[(df1.date.min()<tdf1['date'])&(tdf1['date']<df1.date.max())]
        for ii in range(self.nrows):
            self.ax.clear()
            self.ax2.clear()
            vv = df1.close.tolist()
            if len(vv)<3:
                log.error("skip displaying nearly empty frame")
                continue
            yp1 = self.ax.plot(df1.date, df1.close, label=self.Sym)
            sdf = tdf1.loc[tdf1['quantity']<0]
            bdf = tdf1.loc[tdf1['quantity']>0]
            sp1 = self.ax.scatter(sdf.date, sdf.price, color='r', marker='^')
            bp1 = self.ax.scatter(bdf.date, bdf.price, color='g', marker='v')
            yp2 = self.ax2.plot(df1.date, df1.alpha, 'g--')
        self.ax.xaxis.set_major_formatter(mds.DateFormatter('%Y%m%d'))
        plt.setp(self.ax.get_xticklabels(), visible=True, rotation=45)
        plt.draw()

    def view(self):
        #fig, ax = plt.subplots(nrows=self.nrows,ncols=1,figsize=(10, 6),squeeze=False)
        fig, ax = plt.subplots(figsize=(10, 6))
        self.ax = ax
        self.ax2 = ax.twinx()
        fig.canvas.mpl_connect('button_press_event', self.on_event)
        self.fig = fig
        if self._df is not None:
            self.viewAt()
            plt.show(block=True)
        else:
            self.error = "ERROR: cannot display empty data"
            log.error(self.error)
