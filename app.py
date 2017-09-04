#encoding:utf-8
import pytz
from flask import Flask,render_template,request, render_template, jsonify
from flask.ext.bootstrap import Bootstrap
from flask.ext.script import Manager
from flask.ext.moment import Moment
from datetime import datetime
import sqlite3
from flask import Flask,request
from flask import jsonify
from flask import json
import json
from flask.ext.pymongo import PyMongo
import datetime
import time
from bson import json_util
import tushare as ts
import pandas as pd
from zipline import TradingAlgorithm
from zipline.api import order, sid,get_datetime
from zipline.data.loader import load_data
from zipline.api import order_target, record, symbol, history, add_history,symbol,set_commission,order_percent,set_long_only,get_open_orders
from zipline.finance.commission import OrderCost
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
import pymongo
import httplib2
from pandas import DataFrame
# gevent
from gevent import monkey
from gevent.pywsgi import WSGIServer
monkey.patch_all()

from cStringIO import StringIO
import sys

app = Flask(__name__)
manager = Manager(app)
bootstrap = Bootstrap(app)
moment = Moment(app)

app.config['MONGO_HOST'] = '127.0.0.1'
app.config['MONGO_PORT'] = 27017
mongo = PyMongo(app)

client = pymongo.MongoClient('127.0.0.1',27017)
oriprice=client.stockoriginalprice    #股票原始数据
stocks=oriprice.collection_names()
stocklist=[]
# for s in stocks:
#     stocklist.append(s[2:8])
# client.close()


input_data = load_data(stockList=['000919'],start="1990-01-01",end="2016-01-16")

print input_data

def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%Y-%m-%d")
        return True
    except:
        return False
def toJson(data):
	return json.dumps(data, default=json_util.default)

# @app.errorhandler(404)
# def page_not_found(e):
#     return render_template('404.html'),404



@app.route("/", methods=["GET"])
def index():
    entry=[]

    # 指数行情
    alldatabase=mongo.db.client.basirealtimepool.collection_names()
    
    for con in alldatabase:
       data=mongo.db.client.basirealtimepool[con].find()
       for d in data:
           #print d
           if con =='hz300':
                entry.append({'name':con,'ape':round(float(d['ape']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           elif con=='sz':
                entry.append({'name':con,'ape':round(float(d['ape']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           elif con == 'small':
                 entry.append({'name':con,'ape':round(float(d['ape']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           elif con == 'create':
                entry.append({'name':con,'ape':round(float(d['ape']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           elif con == 'dp':
                entry.append({'name':con,'ape':round(float(d['ape']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           

    # 行业信息
    career=[]
    #self.client.industry
    alldatabase = mongo.db.client.industryrealtimepool.collection_names()
    for con in alldatabase:
           if 'detail' in con:
               #print con
               continue
           data=mongo.db.client.industryrealtimepool[con].find()
           for d in data:
               career.append({'name':con,'ape':round(float(d['ape']),2),'apb':round(float(d['apb']),2),'apc':round(float(d['apc']),2),'aps':round(float(d['aps']),2),'now':float(d['now']),'diff':round(float(d['diff']),2),'day1':round(float(d['5day']),2),'day2':round(float(d['10day']),2),'day3':round(float(d['30day']),2),'day4':round(float(d['90day']),2),'up':d['up'],'down':d['down']})
           
         
    return render_template("index.html",entry=entry,career=career)

'''
API 下载某只股票的行情数据
ex: http://127.0.0.1:5000/stock/sz002565
/stock/stockname?start=1990-01-01,end=2001-01-01
'''
@app.route('/stock/<stockname>')
def returnStockInfo(stockname):
    start = request.args.get('start')
    end=request.args.get('end')
    if start==None or is_valid_date(start)==False:
        start=datetime.datetime.strptime("1990-01-01", "%Y-%m-%d")
    else:
		start=datetime.datetime.strptime(start, "%Y-%m-%d")
    if end == None or is_valid_date(end)==False:
		now=time.time()
		end=time.strftime('%Y-%m-%d',time.localtime(time.time()))
    else:
		end=datetime.datetime.strptime(end, "%Y-%m-%d")
    
    data1=mongo.db.client.stockoriginalprice[stockname].find().sort("date")

    

    json_results = []
    mongo.db.client.close()
    datedata=[]
    opendata=[]
    closedata=[]
    highdata=[]
    lowdata=[]
    for result in data1:
        result['date']=result['date'].strftime('%Y/%m/%d')
        datedata.append(result['date'])
        opendata.append(result['open'])
        closedata.append(result['close'])
        highdata.append(result['high'])
        lowdata.append(result['low'])
    return jsonify(date=datedata,open=opendata,close=closedata,high=highdata,low=lowdata)


# 重定向到具体股票的页面
@app.route('/stockdetail/<code>')
def show_user_profile(code):
    
    #分割股票的信息
    info = code.split('-') 
    # 得到股票对应的URL
    if info[0][0]=='3' or info[0][0]=='0':
        tmpcode = 'sz'+info[0]
    else:
        tmpcode='sh'+info[0]

    
    entry=[]
    url="/stock/"+tmpcode
    entry.append({'url':url})
    return render_template("stockdetail.html",title=info[1],entry=entry)



'''
获取指数类型的股票池
'''
@app.route('/indexlist/<kind>')
def getIndexlist(kind):
    datadetail=mongo.db.client.basirealtimepool[kind].find()
    entry=[]
    for d in datadetail:
        entry.append({'name':d['name'],'code':d['code'],'pe':round(d['pe'],2),'pb':round(d['pb'],2),'pc':round(d['pc'],2),'ps':round(d['ps'],2),"change":round(d['change'],2),'turnoverratio':round(d['turnoverratio'],2),"volume":round(d['volume'],2),"marketcap":round(d['marketcap'],2),"day1":round(d['5day'],2),"day2":round(d['10day'],2),"day3":round(d['30day'],2),"day4":round(d['90day'],2)})
    if kind =='hz300detail':
        mytitle=u"沪深300"
    elif kind == 'szdetail':
        mytitle=u"上证50"
    elif kind == 'smalldetail':
        mytitle=u"中小板"
    elif kind == 'createdetail':
        mytitle=u"创业板"
    elif kind == 'dpdetail':
        mytitle=u"大盘"

    return  render_template('stockclass.html', title=mytitle,entry=entry)


@app.route('/industrylist/<kind>')
def getIndustrylist(kind):
    
    con=kind+'detail'
    #print con
    datadetail=mongo.db.client.industryrealtimepool[con].find()
    entry=[]
    
    for d in datadetail:
        entry.append({'name':d['name'],'code':d['code'],'pe':round(float(d['pe']),2),'pb':round(float(d['pb']),2),'ps':round(float(d['ps']),2),'pc':round(float(d['pc']),2),"change":round(float(d['change']),2),'turnoverratio':round(float(d['turnoverratio']),2),"volume":round(float(d['volume']),2),"marketcap":round(float(d['marketcap']),2),"day1":round(float(d['5day']),2),"day2":round(float(d['10day']),2),"day3":round(float(d['30day']),2),"day4":round(float(d['90day']),2)})

    return  render_template('stockclass.html', title=kind,entry=entry)








'''
API 返回股票列表
'''
@app.route('/stocklist/<kind>')
def returnall(kind):
	json_results = []
	if kind not in ['hz300','all','zz500','sz50','st']:
		kind='all'
	data=mongo.db.client.pool[kind].find()
	for result in data:
		if kind =='all':
			json_results.append(result['codes'])
		else:
			json_results.append(result['code'])
	return toJson(json_results)





@app.route('/index/<indexname>')
def returnindexname(indexname):
	start = request.args.get('start')
	end=request.args.get('end')
	if start==None or is_valid_date(start)==False:
		start=datetime.datetime.strptime("1990-01-01", "%Y-%m-%d")
	else:
		start=datetime.datetime.strptime(start, "%Y-%m-%d")
	if end == None or is_valid_date(end)==False:
		now=time.time()
		end=time.strftime('%Y-%m-%d',time.localtime(time.time()))
	else:
		end=datetime.datetime.strptime(end, "%Y-%m-%d")
	data1=mongo.db.client.index[indexname].find()
	json_results = []

	for result in data1:
		json_results.append(result)
	return toJson(json_results)


# 主观选股
@app.route('/select')
def ownselect():
    return render_template("ownselect.html")



# 量化回测
@app.route('/startbacktest')
def startbacktest():
    starttime = request.args.get('starttime')
    endtime = request.args.get('endtime')
    code = request.args.get('code')
    #处理代码
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = mystdout = StringIO()
    sys.stderr = mystderr = StringIO()

    try:
        algo = TradingAlgorithm(script=code, startdate=starttime,enddate=endtime,capital_base=10000,benchmark='sz399004')
        results = algo.run(input_data)
        print results
    except Exception as error:
        print('caught this error: ' + repr(error))
         
    sys.stdout = old_stdout
    sys.stderr = old_stderr
    print mystdout.getvalue()
    print mystderr.getvalue()

    json_results = []
    return toJson(json_results)


# 量化回测
@app.route('/research')
def research():
    #start=datetime(2015, 12, 10, tzinfo=pytz.utc),end=datetime(2016, 12, 10, tzinfo=pytz.utc),
    #algo = TradingAlgorithm(script=code, startdate="2015-11-20",enddate="2016-12-25",capital_base=10000,benchmark='sz399004')
    #results = algo.run(input_data)
    #print results
    return render_template("research.html")



# 按照指标选股
@app.route('/stocks/')
def stocksselect():
    # 获取各个参数
    peup = float(request.args.get('peup'))
    pedown = float(request.args.get('pedown'))


    psup = float(request.args.get('psup'))
    psdown = float(request.args.get('psdown'))


    pbup = float(request.args.get('pbup'))
    pbdown = float(request.args.get('pbdown'))


    pcup = float(request.args.get('pcup'))
    pcdown = float(request.args.get('pcdown'))



    onedayup=float(request.args.get('onedayup'))
    onedaydown=float(request.args.get('onedaydown'))


    fivedayup=float(request.args.get('fivedayup'))
    fivedaydown=float(request.args.get('fivedaydown'))


    tendayup=float(request.args.get('tendayup'))
    tendaydown=float(request.args.get('tendaydown'))


    thirtydayup=float(request.args.get('thirtydayup'))
    thirtydaydown=float(request.args.get('thirtydaydown'))


    nightydayup=float(request.args.get('nightydayup'))
    nightydaydown=float(request.args.get('nightydaydown'))

    marketup=float(request.args.get('marketup'))
    markedown=float(request.args.get('marketdown'))
    turnoverdown=float(request.args.get('turnoverdown'))
    turnoverup=float(request.args.get('turnoverup'))

    

    #从数据库进行选取
    alldatabase=mongo.db.client.pricetime.collection_names()
    timelist = []
    for t in alldatabase:
        timelist.append(datetime.datetime.strptime(t, "%Y-%m-%d"))
        
    #按照时间先后顺序排列
    timelist.sort()

    day=timelist[-1].strftime('%Y-%m-%d')       


    data1=mongo.db.client.pricetime[day].find({"$and":[{"PE_TTM": {"$gte": pedown,"$lt":peup}},{"PS_TTM": {"$gte": psdown,"$lt":psup}},{"PC_TTM": {"$gte": pcdown,"$lt":pcup}},{"PB": {"$gte": pbdown,"$lt":pbup}},{"change": {"$gte": onedaydown,"$lt":onedayup}}]})


    
    codes=[]
    json_results = []
    for result in data1:
        dapandata=mongo.db.client.basirealtimepool['dpdetail'].find({"code":result['code'][2:8]})
        for d in dapandata:
            #print d
            if d['change']<=onedayup and d['change']>= onedaydown and d['5day'] <=fivedayup and d['5day']>= fivedaydown and d['10day'] <=tendayup and d['10day']>= tendaydown and d['30day'] <=thirtydayup and d['30day']>= thirtydaydown and d['90day'] <= nightydayup and d['90day']>= nightydaydown and d['turnoverratio']>=turnoverdown and d['turnoverratio']<= turnoverup and d['marketcap'] >= markedown and d['marketcap']<= marketup :
                json_results.append({"code":d['code'],"change":round(d['change'],2),"stockname":d['name']})

    
    return toJson(json_results)







if __name__ == "__main__":
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()
    #manager.run()