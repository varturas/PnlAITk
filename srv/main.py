import sys,os
from lib.Portfolio import *
import traceback
import logging as log
import os.path,math
import html
from bokeh.layouts import row,column,layout,widgetbox
from bokeh.models import Button,ColumnDataSource,Span,Label,OpenURL,Paragraph
from bokeh.models.widgets import TextInput,DataTable,TableColumn,Div,DateFormatter,NumberFormatter,Paragraph,DatePicker
from bokeh.models.widgets import Select
from bokeh.models import Range1d, LinearAxis
from bokeh.plotting import figure,curdoc
from datetime import datetime as DT
from datetime import date, timedelta
import numpy as np

log.getLogger().setLevel(log.WARN)
#log.getLogger().setLevel(log.DEBUG)

portX = None
# page elements
ww=700
hh=70
symBox = TextInput(title="Pick a Symbol and date range",  value='BTCUSD', height=hh)
todayDate = DT.today().date()
nDaysAgo = 200
datePick1=DatePicker(title='start date',min_date=date(2005,3,23),max_date=date(2022,12,31),value=todayDate-timedelta(days=nDaysAgo),height=hh)
datePick2=DatePicker(title='end date',min_date=date(2005,3,23),max_date=date(2022,12,31),value=todayDate,height=hh)
submit1 = Button(label='Submit',width=20,height=100)
stratBox = Select(title="Alpha",value='WMA',options=['BuyAndHold','RSI'],width=20)
buyPriceBox = Select(title="Buy Price",value='close',options=['open','close'])
buyQtyBox = TextInput(title="Buy Quantity",  value='1', height=hh)
buyCondBox = TextInput(title="Buy Condition",  value='', height=hh)
sellPriceBox = Select(title="Sell Price",value='close',options=['open','close'])
sellQtyBox = TextInput(title="Sell Quantity",  value='1', height=hh)
sellCondBox = TextInput(title="Sell Condition",  value='', height=hh)
submit2 = Button(label='Submit',width=20,height=hh)
#errBox  = Div(text="",width=ww,height=hh//2,style={'overflow-y':'scroll','height':'150px'})
errBox  = Div(text="",width=ww,height=hh//2)
lineSrc = ColumnDataSource(data={'date':[],'close':[]})
sellSrc = ColumnDataSource(data={'x':[],'y':[]})
buySrc  = ColumnDataSource(data={'x':[],'y':[]})
alphaSrc = ColumnDataSource(data={'date':[],'alpha':[]})
retPlot = figure(plot_width=250,plot_height=100,x_axis_type='datetime', y_axis_label='price')
#retPlot.extra_y_ranges['alpha'] = Range1d(start=-10, end=100)
#retPlot.add_layout(LinearAxis(y_range_name='alpha', axis_label='alpha'), 'right')
p1 = Paragraph(text="A table below shows performance of trading algorithms for chosen symbols.",width=ww,height=hh//2)
p2 = Paragraph(text="In this section you can customize parameters of a chosen strategy. First choose a strategy from a dropdown, then change parameters in the input forms and save. The strategy will show up as \"Custom\" in a summary table. Note that Save button only available for logged in and registered users.", width=int(1.2*ww),height=hh//2)
p3 = Paragraph(text="A graph below displays a price of a chosen stock symbol (tanned line), alongside with alpha for a chosen strategy (in orange). Buy trades are shown as blue upward triangles, and sale trades are red downward triangles.", width=int(1.2*ww),height=hh//2)

emptyResult = dict(name=[],pnl=[],cnt=[])
summaryTableSource = ColumnDataSource(data=emptyResult)
summaryColumns = [
        TableColumn(field='name',title='Name',width=120),
        TableColumn(field='pnl',title='P&L',formatter=NumberFormatter(format='0.00%',text_align='right'),width=80),
        TableColumn(field='cnt',title='Count',formatter=NumberFormatter(format='0,0',text_align='right'),width=80)
        ]
summaryTbl = DataTable(source=summaryTableSource,columns=summaryColumns,width=ww,height=200)
account = None
save1 = Button(label='Save',width=20,height=hh)

class Args:
    def __init__(self,args=None):
        log.debug('init Args')
        self.args = args

# actions
def lookupRecs1():
    sym = symBox.value
    errBox.text = 'Done'
    return lookupRecs(sym,"NA")
def lookupRecs2():
    sym = symBox.value
    customStrat = {'alphaName':stratBox.value, 'buyCondition': buyCondBox.value, 'buyPrice': buyPriceBox.value, 'buyQuantity': buyQtyBox.value, 'sellCondition': sellCondBox.value, 'sellPrice': sellPriceBox.value, 'sellQuantity': sellQtyBox.value }
    customStratText = '[STRATEGY],'+','.join(['{}={}'.format(kk,vv) for kk,vv in customStrat.items()])
    log.warn(customStratText)
    errBox.text = html.escape(customStratText)
    #return lookupRecs(sym,"NA")
    return lookupRecs(sym,customStratText)
def lookupRecs(sym=None, custom=None):
    global retPlot,lineSrc,sellSrc,buySrc,alphaSrc,portX
    summaryTableSource.data = emptyResult
    lineSrc.data = dict(date=[],close=[])
    dt1 = datePick1.value
    dt2 = datePick2.value
    Dates,df = [],None
    if dt1: Dates.append(dt1)
    if dt2: Dates.append(dt2)
    # create portfolio
    args = Args()
    args.view = None; args.force = True
    args.backtest = custom
    args.dates = Dates
    args.Syms = args.Sym = symBox.value
    if account: args.account = account
    portX = PNLPortfolios(args)
    portDF = portX.getPortDF()
    iStrat = portDF.index[portDF['name']!='Custom'][0] # get 1st match other than Custom
    if custom == "NA":
        stratName1 = portDF.iloc[iStrat]['name']
        stratBox.value = stratName1
    else: stratName1 = stratBox.value
    Sym1 = portDF.iloc[iStrat]['sym']
    strat1 = portX.getStrategy(name=stratName1)
    buyCondBox.value = strat1.buyCondition
    sellCondBox.value = strat1.sellCondition
    if portDF is None or not isinstance(portDF, pd.DataFrame) or portDF.empty:
        errBox.text = error = "Cannot find any results, try again..." 
        return
    df = portX.getDF(Sym=Sym1)
    df['alpha'] = strat1.getAlpha()
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        errBox.text = error = "Cannot find any graph data, try again..." 
    tDF = portX.getTrades(Sym=Sym1)
    DT1 = DT(dt1.year,dt1.month,dt1.day) - timedelta(days=2)
    DT2 = DT(dt2.year,dt2.month,dt2.day) + timedelta(days=1)
    subDF = df.loc[(df['date'] > DT1.date()) & (df['date'] <= DT2.date())]
    tradeDF = tDF.loc[(tDF['date'] > DT1.date()) & (tDF['date'] <= DT2.date()) & (tDF['quantity']!=0)]
    minI,maxI = None,None
    closes = subDF['close'].tolist()
    if len(closes): minI,maxI = min(closes),max(closes)
    lineSrc.data = subDF[['date','close']].to_dict('list')
    alphaSrc.data = subDF[['date','alpha']].to_dict('list')
    lastDate = tradeDF.date.max()
    idx_cond = tradeDF.groupby("stratName").apply(lambda x,DT1: x.where((x.date==lastDate)&(x.quantity!=0)&(x.action.replace('force_close','').replace(',','')!='')).last_valid_index(),DT1=lastDate)
    if not idx_cond.empty:
        actDF = tradeDF.loc[tradeDF.index.intersection(idx_cond)]
        actDF['now']='-'
        actDF.loc[actDF.quantity>0,'now'] = 'up'
        actDF.loc[actDF.quantity<0,'now'] = 'down'
        actDF.loc[actDF.stratName=='BuyAndHold','now'] = 'hold'
        actDF = actDF.rename(columns={'stratName':'name'})
        portDF = pd.merge(portDF, actDF[['name','now']], left_on=['name'], right_on=['name'], how='left')
        portDF[['now']] = portDF[['now']].fillna('-')
    else:
        portDF['now'] = '-'
    summaryTbl.columns = []
    portDF = portDF[['name','pnl','sharpe','sym','days','cnt','now']]
    portDF = portDF.rename(columns={'pnl':'return'})
    for col1 in [col1 for ii,col1 in enumerate(portDF)]:
        if portDF[col1].dtype==np.float64:
            if col1=='return':
                summaryTbl.columns.append(TableColumn(field=col1, title=col1, formatter=NumberFormatter(format='0[.]00%',text_align='right')))
            else:
                summaryTbl.columns.append(TableColumn(field=col1, title=col1, formatter=NumberFormatter(format='0[.]00',text_align='right')))
        else:
            summaryTbl.columns.append(TableColumn(field=col1, title=col1))
    summaryTableSource.data = portDF.to_dict('list')
    sellData = tradeDF.loc[tradeDF['quantity']<0]
    buyData = tradeDF.loc[tradeDF['quantity']>0]
    if not stratName1 in ['_BuyAndHold_']:
        sellData =  sellData.loc[sellData['stratName']==stratName1]
        buyData =  buyData.loc[buyData['stratName']==stratName1]
    sellSrc.data = dict(x=sellData['date'].tolist(),y=sellData['price'].tolist())
    buySrc.data = dict(x=buyData['date'].tolist(),y=buyData['price'].tolist())
    retPlot.line(x='date',y='close',line_color='tan',source=lineSrc,name='bench')
    retPlot.triangle(x='x',y='y',size=7,fill_color='lightskyblue',source=buySrc,name='trades')
    retPlot.inverted_triangle(x='x',y='y',size=7,fill_color='red',source=sellSrc,name='trades')
    aMin,aMax = subDF.alpha.min(),subDF.alpha.max()
    if False and aMin and aMax and not math.isnan(aMin):
        retPlot.extra_y_ranges['alpha'].start = aMin
        retPlot.extra_y_ranges['alpha'].end = aMax
        retPlot.line(x='date',y='alpha',line_color='orange',y_range_name='alpha',source=alphaSrc,name='alpha')
    retPlot.x_range.start,retPlot.x_range.end=DT1.timestamp()*1000,DT2.timestamp()*1000

def saveStrat():
    if not account:
        errBox.text = 'Cannot save without account info'
        return None
    else:
        errBox.text = 'Saving custom strategy to '+ account
    customStrat = {'alphaName':stratBox.value, 'buyCondition': buyCondBox.value, 'buyPrice': buyPriceBox.value, 'buyQuantity': buyQtyBox.value, 'sellCondition': sellCondBox.value, 'sellPrice': sellPriceBox.value, 'sellQuantity': sellQtyBox.value }
    customStratText = '[STRATEGY],'+','.join(['{}={}'.format(kk,vv) for kk,vv in customStrat.items()])
    log.warn('saving ' + customStratText)
    errBox.text = 'Saving to '+ account + ' ' + html.escape(customStratText)
    portX.setStrategy(account)

def populateParams(attr, old, new):
    strat1 = portX.getStrategy(name=stratBox.value)
    buyCondBox.value = strat1.buyCondition
    buyPriceBox.value = strat1.buyPrice
    buyQtyBox.value = strat1.buyQuantity
    sellCondBox.value = strat1.sellCondition
    sellPriceBox.value = strat1.sellPrice
    sellQtyBox.value = strat1.sellQuantity
    lookupRecs2()

sym_overlay = row(symBox, datePick1, datePick2, column(Div(),submit1,height=hh),height=hh)
#strat_overlay = row(stratBox, column(Div(),submit2,height=hh),height=hh)
buy_overlay = row(buyCondBox,buyPriceBox,buyQtyBox,stratBox)
sell_overlay = row(sellCondBox,sellPriceBox,sellQtyBox,column(Div(),save1,height=hh))
#err_overlay = row(column(row(save1, errBox)))
err_overlay = row(errBox)

# assemble the page
def assemble_page():
    curdoc().clear()
    l1 = layout([ \
        [sym_overlay], \
        [p1], \
        [summaryTbl], \
        [p2], \
        [buy_overlay], [sell_overlay], \
        [err_overlay], \
        [p3], \
        [retPlot], \
        ],sizing_mode='scale_width')
        #],sizing_mode='stretch_both')
    if not account: save1.disabled=True
    else: save1.disabled=False
    curdoc().add_root(l1)
    curdoc().title = "P&L.AI"
    if os.environ.get('DJANGO_DEVELOPMENT'): dj_URL = "http://127.0.0.1:8000/"
    else: dj_URL = "http://pnlai-env.eba-b6bihwb7.us-west-2.elasticbeanstalk.com/"
    curdoc().template_variables["logout"] = dj_URL + 'logout/'
    curdoc().template_variables["login"] = dj_URL + 'pnlaiapp/user_login/'
    curdoc().template_variables["register"] = dj_URL + 'pnlaiapp/register/'

def getTop(Str):
    DF = None
    tday = DT.now().strftime('%Y-%m-%d')
    t_f1,t_fs = None,glob.glob('data/store/yf/{}_*.pkl'.format(Str))
    if t_fs and len(t_fs)>0: t_f1 = max(t_fs, key=os.path.getctime)
    if t_f1 and os.path.isfile(t_f1):
        dtStr1 = t_f1.split('/store/yf/')[-1].split('.pkl')[0].split('_')[-1]
        dt1 = (datetime.datetime.strptime(dtStr1,'%Y-%m-%d')).date()
        if dt1 > (DT.today()-timedelta(days=30)).date():
            DF = pd.read_pickle(t_f1)
    return DF

def getSyms():
    SymLs = []
    gDF = getTop('gainers')
    if isinstance(gDF, pd.DataFrame): SymLs.append(gDF.iloc[0]['Symbol'])
    lDF = getTop('losers')
    if isinstance(lDF, pd.DataFrame): SymLs.append(lDF.iloc[0]['Symbol'])
    return SymLs

# assign actions
def main():
    global account
    # check if there is a list of initial syms
    SymLs = []
    args_g = curdoc().session_context.request.arguments
    try: SymLs = [aa.decode("utf-8") for aa in args_g.get('Sym')]
    except: SymLs = getSyms()
    try: account = args_g.get('account')[0].decode("utf-8")
    except: account = None
    if SymLs: syms = ','.join(SymLs)
    else: syms = "BTCUSD"
    errBox.text = 'Starting lookup'
    datePick1.value=(DT.now()-timedelta(days=nDaysAgo)).strftime('%Y-%m-%d')
    datePick2.value=DT.now().strftime('%Y-%m-%d')
    symBox.value = syms
    lookupRecs(syms,"NA")
    submit1.on_click(lookupRecs1)
    #submit2.on_click(lookupRecs2)
    save1.on_click(saveStrat)
    stratBox.on_change('value', populateParams)
    assemble_page()
    errBox.text = 'Completed lookup'

if __name__ == '__main__':
    if log.DEBUG>=log.getLogger().getEffectiveLevel():
        backtest="[STRATEGY],alphaName=WMA,buyCondition=lastClose<alpha and close>alpha,buyPrice=close,buyQuantity=1,sellCondition=lastClose>alpha and close<alpha,sellPrice=close,sellQuantity=1"
        #backtest = "NA"
        datePick1.value="2020-12-01";datePick2.value="2021-04-01"; lookupRecs("BTCUSD",backtest)
        account='test'; saveStrat()
else:
    main()
