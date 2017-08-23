#encoding:utf-8
import sys
import numpy as np
#verion1: get all companies data from tushare and store them in Mongodb
import pymongo
import datetime
import tushare as ts
import time
import json
import pandas as pd
from collections import OrderedDict
import pytz
import types 
import requests
from io import BytesIO, StringIO
import os
import click
import re
from os import listdir
from os.path import isfile, join
from os import walk
from pandas import DataFrame
import constants
import math
import time

'''
 手动插入每天股票信息，每天进行操作，把当天的数据存入todaydata文件夹中，然后执行完本脚本以后，把
 todaydata数据手动移动到newdata中
'''
class DayOperation:
    
    todaydata=constants.todaydata
    

    def __init__(self,Ip,port):
        self.ip=Ip
        self.port=port

    #股票按照时间进行索引存储，便于计算指数
    def storagebyTime(self):
        
        onlyfiles = [ f for f in listdir(self.todaydata) if isfile(join(self.todaydata,f)) ]
        
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='i':
                continue

            self.pricetime[fdate].drop()
            
       
        #read from using pandas
        totaltables=self.pricetime.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='i':
                continue
            #self.pricetime[fdate].drop()

            df = pd.read_csv(self.todaydata+"/"+f)
            if len(f) < 12:
                continue
            if fdate in totaltables:
                print fdate+u"的股票数据已经存在"
                continue

            #df.index = df.code
            print u"插入"+fdate+u"的股票数据"
            records = json.loads(df.T.to_json()).values()
            self.pricetime[fdate].insert_many(records) 

    
   
    def storagedayprice(self):
        
         # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.todaydata) if isfile(join(self.todaydata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='i':
                continue

            df = pd.read_csv(self.todaydata+"/"+f)
            if len(f) < 12:
                continue


            codes=df.code
            series={}


            for i in range(0,len(codes)):
                code=df.iloc[i].code
                series["open"]=df.iloc[i].open
                series["high"]=df.iloc[i].high
                series["low"]=df.iloc[i].low
                series["close"]=df.iloc[i].close
                series["change"]=df.iloc[i].change
                series["volume"]=df.iloc[i].volume
                series["money"]=df.iloc[i].money
                series["traded_market_value"]=df.iloc[i].traded_market_value
                series["market_value"]=df.iloc[i].market_value
                series["adjust_price"]=df.iloc[i].adjust_price
                series["report_type"]=df.iloc[i].report_type
                series["PE_TTM"]=df.iloc[i].PE_TTM
                series["PS_TTM"]=df.iloc[i].PS_TTM
                series["PC_TTM"]=df.iloc[i].PC_TTM
                series["PB"]=df.iloc[i].PB
                series["adjust_price_f"]=df.iloc[i].adjust_price_f
                series['date'] = datetime.datetime.strptime(df.iloc[i].date, "%Y-%m-%d")
                name=code

                #检查一下今天数据是否存在
                countstock=self.oriprice[name].find({"date":series['date']}).count()
                if countstock ==0:
                    print (u"插入股票"+name+" "+str(series["date"])+u"数据")
                    self.oriprice[name].insert_one(series)
                else:
                    print(u"股票"+name+" "+str(series['date'])+u"已经存在")


        # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.todaydata) if isfile(join(self.todaydata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='d':
                continue

            df = pd.read_csv(self.todaydata+"/"+f)
            if len(f) < 12:
                continue

            codes=df.index_code
            series={}


            for i in range(0,len(codes)):
                code=df.iloc[i].index_code
                series["open"]=df.iloc[i].open
                series["high"]=df.iloc[i].high
                series["low"]=df.iloc[i].low
                series["close"]=df.iloc[i].close
                series["change"]=df.iloc[i].change
                series["volume"]=df.iloc[i].volume
                series["money"]=df.iloc[i].money
                series['date'] = datetime.datetime.strptime(df.iloc[i].date, "%Y-%m-%d")
                name=code
                #check exits
                countstock=self.index[name].find({"date":series['date']}).count()
                if countstock ==0:
                    print (u"插入指数"+name+str(series["date"])+u"数据")
                    self.index[name].insert_one(series)
                else:
                    print(u"指数"+name+" "+str(series['date'])+u"已经存在")


                    
            
     #每天计算一次，计算行业的指数
    def ComputeIndustryIndexDay(self):
        alldatabase=self.pricetime.collection_names()
         
        timelist = []
        for t in alldatabase:
            timelist.append(datetime.datetime.strptime(t, "%Y-%m-%d"))
        
        #按照时间先后顺序排列
        timelist.sort()

        #每个指数初始化为1000
        allindustry=self.industry.collection_names()  
        for i in allindustry:
            indexname = i+"detail"
            self.industryindex[indexname].drop()

        for i in allindustry:
            indexstart = 1000
            codes = self.industry[i].find()
            listcodes=[]
            for code in codes:
                listcodes.append(code)
            for d in timelist:
                day=d.strftime('%Y-%m-%d')
                detail=self.pricetime[day].find()
                series={"code":[],"change":[],"market":[],"pe":[]}
                for de in detail:
                    series["code"].append(de["code"])
                    series["change"].append(de["change"]) 
                    series["market"].append(de["market_value"])
                    series["pe"].append(de["PE_TTM"])
                #print len(series["code"])


                totaldata=zip(series['change'],series["market"],series["pe"])
                stockdf = pd.DataFrame(data=list(totaldata),index=series["code"],columns = ['change','market',"pe"])
               
                #原始数据存在重复的行，这里去掉重复数据
                stockdf=stockdf.drop_duplicates()

                todaydiff = 0
                summarket = 0
                totalpe = 0
                n = 0
                for code in listcodes:
                    if code.keys()[0][0]=='3' or  code.keys()[0][0]=='0' or code.keys()[0][0]=='6':
                        c = code.keys()[0]
                    else:
                        c =code.keys()[1]

                    if c[0]=='3' or  c[0]=='0':
                        tmp = 'sz'+c
                    else:
                        tmp='sh'+c

                    if tmp in stockdf.index:
                        if isinstance(stockdf.ix[tmp].change, np.float) and isinstance(stockdf.ix[tmp].market, np.float):
                            todaydiff =todaydiff+stockdf.ix[tmp].change*stockdf.ix[tmp].market
                            summarket =summarket+stockdf.ix[tmp].market
                        if isinstance(stockdf.ix[tmp].pe, np.float) and stockdf.ix[tmp].pe > 0 and stockdf.ix[tmp].pe < 500:
                            totalpe +=stockdf.ix[tmp].pe
                            n +=1

                if summarket == 0:
                    indexstart = 1000
                else:
                    indexstart *= (1+todaydiff/summarket)

                if n == 0:
                    averagepe = 0
                else:
                    averagepe = totalpe/n;
                #print averagepe
                indexname = i+"detail"
                dict1={"time":d,"value":indexstart,"pe":averagepe}
                print dict1
                self.industryindex[indexname].insert(dict1)


    ## 数据库连接设置
    def Conn(self):
        
        self.client = pymongo.MongoClient(self.ip,self.port)

        self.index=self.client.index #storage index

        self.industry = self.client.industry # 存储行业信息
        self.industryindex = self.client.industryindex #行业指数信息


        self.oriprice=self.client.stockoriginalprice    #股票原始数据
        self.pricetime = self.client.pricetime     #按照时间进行存储

        self.realtimepool = self.client.basirealtimepool    #存储当天的行情


        self.industryindex = self.client.industryindex #行业指数信息

        self.indestrytimepool =self.client.industryrealtimepool  # 存储行业指数实时行情




    def getReal(self):
        alldatabase=self.pricetime.collection_names()
        timelist = []
        for t in alldatabase:
            timelist.append(datetime.datetime.strptime(t, "%Y-%m-%d"))
        
        #按照时间先后顺序排列
        timelist.sort()

        day=timelist[-1].strftime('%Y-%m-%d')       
        realTime = DataFrame(list(self.pricetime[day].find()))
        realTime.index=realTime.code
        tusharedata=ts.get_today_all()
        tusharedata.index= tusharedata.code
        labes=['code','stockname','changepercent','trade','open','high','low','settlement','volume','turnoverratio','amount','per','pb','mktcap','nmc']
        tusharedata.columns=labes

        tushareindex = ts.get_index()
        tushareindex.index = tushareindex.code
        #print tushareindex
        labels=['code','indexname','change','open','preclose','close','high','low','volume','amount']
        tushareindex.columns=labels
        return realTime,tusharedata,tushareindex


    # 获取今天的情形
    def getToday(self,realtime,tusharedata,tushareindex):
        #大盘
        dpindexname='dp'
        dpdetail='dpdetail'
        tusharedata.index= tusharedata.code

        self.insertDetail(realTime,tusharedata,tushareindex,'000001',dpindexname,dpdetail)

        # 沪深300
        hs300indexname='hz300'
        hs300detail='hz300detail'
        self.insertDetail(realTime,tusharedata,tushareindex,'000300',hs300indexname,hs300detail)


        # 上证50
        sz50indexname='sz'
        sz50detail='szdetail'
        self.insertDetail(realTime,tusharedata,tushareindex,'000016',sz50indexname,sz50detail)
        

        # 中小板
        smallindexname='small'
        smalldetail='smalldetail'
        self.insertDetail(realTime,tusharedata,tushareindex,'399005',smallindexname,smalldetail)
        
        
        # 创业板
        createindexname='create'
        createdetail='createdetail'
        self.insertDetail(realTime,tusharedata,tushareindex,'399006',createindexname,createdetail)
        




    def insertDetail(self,realTime,tusharedata,tushareindex,indexcode,dbname,dbdetail):
        
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")


        #获取指数 过去一段时间的特性
        indexhistorydata=ts.get_k_data(code=indexcode,index=True,start=hundredyday,end=today)
        index_5day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-6].close)/indexhistorydata.iloc[-1].close*100
        index_10day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-11].close)/indexhistorydata.iloc[-1].close*100
        index_30day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-31].close)/indexhistorydata.iloc[-1].close*100
        index_90day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-91].close)/indexhistorydata.iloc[-1].close*100
        
        if indexcode =='000016':
            df=ts.get_sz50s()
        elif indexcode =='000300':
            df=ts.get_hs300s()
        elif indexcode == '399005':
            df=ts.get_sme_classified()
        elif indexcode == '399006':
            df=ts.get_gem_classified()
        elif indexcode =='000001':
            df=realTime

        

        ape=0
        up =0
        down =0
        number = 0


        for i in range(0,len(df)):
            code=df.iloc[i].code
            
            #print code,df.iloc[i].name
            series={"date":[],"close":[],"change":[]}
            
            timehundrd = datetime.datetime.strptime(hundredyday, "%Y-%m-%d")
            timetoday =  datetime.datetime.strptime(today, "%Y-%m-%d")

            if code[0]=='3' or code[0]=='0':
                tmpcode = 'sz'+code
            elif code[0]=='6':
                tmpcode='sh'+code
            else:
                tmpcode = code



            for stockdaily in self.oriprice[tmpcode].find({"date": {"$gte": timehundrd,"$lt":timetoday}}).sort("date"):
                series["date"].append(stockdaily["date"])
                series["close"].append(stockdaily["adjust_price"]) 
                series["change"].append(stockdaily["change"])

            
            length = len(indexhistorydata)
            
            for i in range(0,length):
                timestep=datetime.datetime.strptime(indexhistorydata.iloc[i].date, "%Y-%m-%d")            
                if (timestep in series["date"]) ==False:
                    if i !=0:
                        series["date"].insert(i,timestep) 
                        series["close"].insert(i,series["close"][i-1]) 
                        series["change"].insert(i,series["change"][i-1])
                        
                    else:
                        series["date"].insert(i,timestep)

                        # 没有数据
                        if len(series["close"]) ==0:
                             series["close"].insert(i,0)
                             series["change"].insert(i,0)
                        else:
                            series["close"].insert(i,series["close"][0]) 
                            series["change"].insert(i,series["change"][0])
                       


            totaldata=zip(series['close'],series["change"])
            stockdf = pd.DataFrame(data=list(totaldata),index=series["date"],columns = ['close','change'])

            if(stockdf.iloc[-1].close !=0):
                stock_5day= (stockdf.iloc[-1].close-stockdf.iloc[-6].close)/stockdf.iloc[-1].close*100
                stock_10day=(stockdf.iloc[-1].close-stockdf.iloc[-11].close)/stockdf.iloc[-1].close*100
                stock_30day=(stockdf.iloc[-1].close-stockdf.iloc[-31].close)/stockdf.iloc[-1].close*100
                stock_90day=(stockdf.iloc[-1].close-stockdf.iloc[-91].close)/stockdf.iloc[-1].close*100
            else:
                stock_5day=0
                stock_10day=0
                stock_30day=0
                stock_90day=0


            # realtime 中可能没有获取的股票数据，因此加上一个判断是否存在的句子    
            if tmpcode in realTime.index:
                pe = realTime.loc[tmpcode].PE_TTM
                tmp = tusharedata.loc[tmpcode[2:8]].code
                
                
                if tmp not in tusharedata.index:
                    continue

                name = tusharedata.loc[tmp].stockname
                turnoverratio=float(realTime.loc[tmpcode].turnover)*100
                changepercent=float(realTime.loc[tmpcode].change)*100
                if changepercent >0:
                    up+=1
                if changepercent <0:
                    down+=1
                volume = realTime.loc[tmpcode].volume/10000
                makrket = float(realTime.loc[tmpcode].traded_market_value)/100000000
                pb = realTime.loc[tmpcode].PB
                pc = realTime.loc[tmpcode].PC_TTM
                ps = realTime.loc[tmpcode].PS_TTM
                dict1={"code":code,"name":name,"pe":pe,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day,"pb":pb,"pc":pc,"ps":ps}

                
                self.realtimepool[dbdetail].insert(dict1)

                if pe >0 and pe < 1000:
                    ape+=realTime.loc[tmpcode].PE_TTM
                    number +=1
         
        ape /=number


        mydict = {"ape":ape,"now":tushareindex.ix[indexcode].close,"diff":tushareindex.ix[indexcode].change,"5day":index_5day,"10day":index_10day,"30day":index_30day,"90day":index_90day,"up":up,"down":down}
        self.realtimepool[dbname].insert(mydict)
        



       



        

    '''
    关闭数据库
    '''
    def Close(self):
        self.client.close()

    def DeletePool(self):
        alldatabase=self.realtimepool.collection_names()
        for con in alldatabase:
            #print u"delte "+con
            self.realtimepool[con].drop()

        alldatabase=self.indestrytimepool.collection_names()
        for con in alldatabase:
            self.indestrytimepool[con].drop()


    #计算行业的一些信息
    def ComputeRealIndex(self,realTime,tusharedata,tushareindex):
        series = {}

        #获取指数 过去一段时间的特性
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

                
        timehundrd = datetime.datetime.strptime(hundredyday, "%Y-%m-%d")
        timetoday =  datetime.datetime.strptime(today, "%Y-%m-%d")
        indexhistorydata=ts.get_k_data(code='000001',index=True,start=hundredyday,end=today)

        allindustry=self.industry.collection_names()  

        totalnames=[]

        # 对每个行业进行操作
        for i in allindustry:
            up=0
            down=0
            codes = self.industry[i].find()
            listcodes=[]
            todaydiff = 0
            summarket = 0
            totalpe = 0
            totalpb=0
            totalpc=0
            totalps=0
            pen = 0
            psn=0
            pbn=0
            pcn=0
            n=0
            # realtime 中可能没有获取的股票数据，因此加上一个判断是否存在的句子
            industrydetail=i+"detail" 
            for code in codes:
                listcodes.append(code)
                if code.keys()[0][0]=='3' or  code.keys()[0][0]=='0' or code.keys()[0][0]=='6':
                    c = code.keys()[0]
                else:
                    c =code.keys()[1]

                series={"date":[],"close":[],"change":[]}

                if c[0]=='3' or c[0]=='0':
                    tmpcode= 'sz'+c
                elif c[0]=='6':
                    tmpcode='sh'+c
                else:
                    tmpcode=c

                #print tmpcode

                for stockdaily in self.oriprice[tmpcode].find({"date": {"$gte": timehundrd,"$lt":timetoday}}).sort("date"):
                    series["date"].append(stockdaily["date"])
                    #这里读取后复权数据
                    series["close"].append(stockdaily["adjust_price"]) 
                    series["change"].append(stockdaily["change"])

                
                length = len(indexhistorydata)

                for j in range(0,length):
                    timestep=datetime.datetime.strptime(indexhistorydata.iloc[j].date, "%Y-%m-%d")            
                    if (timestep in series["date"]) ==False:
                        if j !=0:
                            series["date"].insert(j,timestep) 
                            series["close"].insert(j,series["close"][j-1]) 
                            series["change"].insert(j,series["change"][j-1])
                            
                        else:
                            series["date"].insert(j,timestep)

                            # 没有数据
                            if len(series["close"]) ==0:
                                series["close"].insert(j,0)
                                series["change"].insert(j,0)
                            else:
                                series["close"].insert(j,series["close"][0]) 
                                series["change"].insert(j,series["change"][0])
                        


                totaldata=zip(series['close'],series["change"])
                stockdf = pd.DataFrame(data=list(totaldata),index=series["date"],columns = ['close','change'])

                #print stockdf

                if(stockdf.iloc[-1].close !=0):
                    stock_5day= (stockdf.iloc[-1].close-stockdf.iloc[-6].close)/stockdf.iloc[-1].close*100
                    stock_10day=(stockdf.iloc[-1].close-stockdf.iloc[-11].close)/stockdf.iloc[-1].close*100
                    stock_30day=(stockdf.iloc[-1].close-stockdf.iloc[-31].close)/stockdf.iloc[-1].close*100
                    stock_90day=(stockdf.iloc[-1].close-stockdf.iloc[-91].close)/stockdf.iloc[-1].close*100
                else:
                    stock_5day=0
                    stock_10day=0
                    stock_30day=0
                    stock_90day=0


                
                
                if tmpcode in realTime.index:
                    
                    pe = realTime.loc[tmpcode].PE_TTM
                    tmp = tusharedata.loc[tmpcode[2:8]].code
                
                
                    if tmp not in tusharedata.index:
                        print tmp +" not in tushare"
                        continue

                    name = tusharedata.loc[tmpcode[2:8]].stockname
                    turnoverratio=float(realTime.loc[tmpcode].turnover)*100
                    changepercent=float(realTime.loc[tmpcode].change)*100
                    if changepercent >0:
                        up+=1
                    if changepercent <0:
                        down+=1
                    volume = realTime.loc[tmpcode].volume/10000
                    makrket = float(realTime.loc[tmpcode].traded_market_value)/100000000
                    pb = realTime.loc[tmpcode].PB
                    pc = realTime.loc[tmpcode].PC_TTM
                    ps = realTime.loc[tmpcode].PS_TTM

                    n +=1

                   
                    dict1={"code":c,"name":name,"pe":pe,"pb":pb,"pc":pc,"ps":ps,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day}
                    todaydiff +=changepercent*makrket/100
                    summarket +=makrket
                    
                    if pe > 0 and pe < 1000:
                        pen+=1
                        totalpe +=pe
                    if pb > 0:
                        pbn+=1
                        totalpb +=pb
                    if pc > 0:
                        pcn+=1
                        totalpc +=pc
                    if ps > 0:
                        psn+=1
                        totalps +=ps
                    #print dict1
                    self.indestrytimepool[industrydetail].insert(dict1)
                # else:
                #     print tmpcode +" not in realTime index "
                #     print code
                #     print c
                
                    
            averagepe=totalpe/pen
            averagepb=totalpb/pbn
            averageps=totalps/psn
            averagepc=totalpc/pcn

            #print averagepe,averagepb,averageps,averagepc

            indexname = i+"detail"

            today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
            hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

            indestrydetail = DataFrame(list(self.industryindex[indexname].find().sort("date")))
            names=['id','pe','time','value']
            indestrydetail.columns=names
            indestrydetail.index=indestrydetail.time

            nowvalue = indestrydetail.iloc[-1].value*(1+todaydiff/summarket)


            industry_5day= (nowvalue-indestrydetail.iloc[-6].value)/nowvalue*100
            industry_10day=(nowvalue-indestrydetail.iloc[-11].value)/nowvalue*100
            industry_30day=(nowvalue-indestrydetail.iloc[-31].value)/nowvalue*100
            industry_90day=(nowvalue-indestrydetail.iloc[-91].value)/nowvalue*100
            
            mydict = {"ape":averagepe,"apb":averagepb,"aps":averageps,"apc":averagepc,"up":up,"down":down,"now":nowvalue,"diff":todaydiff/summarket*100,"5day":industry_5day,"10day":industry_10day,"30day":industry_30day,"90day":industry_90day}
            self.indestrytimepool[i].insert(mydict)

if __name__ == '__main__':
    I=DayOperation(constants.IP,constants.PORT)
    I.Conn()
    I.storagedayprice()
    I.storagebyTime()
    I.ComputeIndustryIndexDay()
    I.DeletePool()
    realTime,tusharedata,tushareindex=I.getReal()
    
    I.getToday(realTime,tusharedata,tushareindex)
    I.ComputeRealIndex(realTime,tusharedata,tushareindex)
    I.Close()