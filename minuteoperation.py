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

# 每n秒执行一次
# add new data and use tushare to update data
class MinuteOperation:
    

    def __init__(self,RemoteIp,RemotePort,LocalIp,LocalPort):
        self.localip    =LocalIp
        self.remoteip   = RemoteIp
        self.remoteport = RemotePort
        self.localport  = LocalPort

    # 获取所有股票的实时行情信息（1）先从远程仓储中获得，如果失败，然后
    def getRealTime(self):
        realTimeName=self.rawstockdata.collection_names()

        timelist = []
        for t in realTimeName:
            timelist.append(datetime.datetime.strptime(t, "%Y-%m-%d-%H:%M:%S"))
        
        #按照时间先后顺序排列
        timelist.sort()

        dbname=timelist[-1].strftime("%Y-%m-%d-%H:%M:%S")


        realTime = DataFrame(list(self.rawstockdata[dbname].find()))
        names=['id','amount','changepercent','code','high','low','mktcap','stockname','nmc','open','pb','per','settlement','trade','turnoverratio','volume']
        realTime.columns=names
        realTime.index=realTime.code

        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")


        realIndex =  DataFrame(list(self.rawindexdata[dbname].find()))
        names=['id','amount','change','close','code','high','low','indexname','open','preclose','volume']
        realIndex.columns = names
        realIndex.index=realIndex.code
        return realTime,realIndex

    # 读取，并且更新实时信息
    def storagepool(self,realTime,realIndex):
        
        # 非交易时间，不操作

        #周六，周日
        # dayOfWeek = datetime.datetime.now().weekday()
        # print "week "+str(dayOfWeek)
        # if dayOfWeek==5 or dayOfWeek==6:
        #     print "it is"+str(dayOfWeek)+"now"
        #     return 
        # minutetime =time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()))
        # #非交易时间
        # hour = float(time.strftime("%H", time.localtime(time.time())))
        # minuite=float(time.strftime("%M", time.localtime(time.time())))
        # print str(hour)+":"+str(minuite)
        # if hour < 9 or hour > 15 or hour==12:
        #     print "1"+str(minuite)
        #     return 
        # elif hour ==11 and minuite > 30:
        #     print "2"+str(minuite)
        #     return
        # elif hour ==9 and minuite <30:
        #     print "3"+str(minuite)
        #     return 

        #计算所有股票的平均PE
        # totalpe = 0
        # totaln = 0
        # for i in range(0,len(realTime)):
        #     pe=realTime.iloc[i].per

        #     if pe > 0  and pe < 200:
        #         totalpe += pe
        #         totaln +=1
        # totalpe/=totaln

        # 获取当前时间
        minutetime =time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()))

        #大盘
        dpindexname=minutetime+'+dp'
        dpdetail=minutetime+'+dpdetail'
        self.insertDetail(realTime,realIndex,'000001',dpindexname,dpdetail)

        # 沪深300
        hs300indexname=minutetime+'+hz300'
        hs300detail=minutetime+'+hz300detail'
        self.insertDetail(realTime,realIndex,'000300',hs300indexname,hs300detail)

        #time.sleep(2)

        # 上证50
        sz50indexname=minutetime+'+sz'
        sz50detail=minutetime+'+szdetail'
        self.insertDetail(realTime,realIndex,'000016',sz50indexname,sz50detail)
        time.sleep(2)

        # 中小板
        smallindexname=minutetime+'+small'
        smalldetail=minutetime+'+smalldetail'
        self.insertDetail(realTime,realIndex,'399005',smallindexname,smalldetail)
        time.sleep(2)
        
        # 创业板
        createindexname=minutetime+'+create'
        createdetail=minutetime+'+createdetail'
        self.insertDetail(realTime,realIndex,'399006',createindexname,createdetail)
        time.sleep(2)

        alldatabase=self.realtimepool.collection_names()
        for con in alldatabase:
            if con not in [hs300indexname,hs300detail,sz50indexname,sz50detail,smallindexname,smalldetail,createindexname,createdetail,dpindexname,dpdetail]:
                 print con+" out"
                 # 这里首先重命名一下，为了防止后面来不及操作，因为删除很慢，所以，会导致后命名时候数据库已经存在信息
                 self.realtimepool[con].drop()

        time.sleep(3)

        alldatabase=self.realtimepool.collection_names()
        for con in alldatabase:
            print con+" in"
            print con.split('+')[1]
            self.realtimepool[con].rename(con.split('+')[1])








    def insertDetail(self,realTime,realIndex,indexcode,dbname,dbdetail):
        
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

        #print df

        ape = 0
        number = 0


        for i in range(0,len(df)):
            code=df.iloc[i].code
            #print code,df.iloc[i].name
            series={"date":[],"close":[],"change":[]}
            if code[0]=='3' or code[0]=='0':
                tmp = 'sz'+code
            else:
                tmp='sh'+code

            
            timehundrd = datetime.datetime.strptime(hundredyday, "%Y-%m-%d")
            timetoday =  datetime.datetime.strptime(today, "%Y-%m-%d")

            for stockdaily in self.oriprice[tmp].find({"date": {"$gte": timehundrd,"$lt":timetoday}}).sort("date"):
                series["date"].append(stockdaily["date"])
                #这里读取后复权数据
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
            if code in realTime.index:
                pe = realTime.loc[code].per
                name = realTime.loc[code].stockname
                turnoverratio=realTime.loc[code].turnoverratio
                changepercent=realTime.loc[code].changepercent
                volume = realTime.loc[code].volume
                makrket = realTime.loc[code].mktcap
                dict1={"code":code,"name":name,"pe":pe,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day}

                self.realtimepool[dbdetail].insert(dict1)

                if pe >0 and pe < 400:
                    ape+=realTime.loc[code].per
                    number +=1
        ape /=number


        mydict = {"ape":ape,"now":realIndex.ix[indexcode].close,"diff":realIndex.ix[indexcode].change,"5day":index_5day,"10day":index_10day,"30day":index_30day,"90day":index_90day}
        self.realtimepool[dbname].insert(mydict)






    def ComputeRealIndex(self,realTime):
        
        series = {}

        #获取指数 过去一段时间的特性
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

                
        timehundrd = datetime.datetime.strptime(hundredyday, "%Y-%m-%d")
        timetoday =  datetime.datetime.strptime(today, "%Y-%m-%d")
        indexhistorydata=ts.get_k_data(code='000001',index=True,start=hundredyday,end=today)

        allindustry=self.industry.collection_names()  

        totalnames=[]
        for i in allindustry:
            codes = self.industry[i].find()
            listcodes=[]
            for code in codes:
                listcodes.append(code)
           



            todaydiff = 0
            summarket = 0
            totalpe = 0
            n = 0
            for code in listcodes:
                if code.keys()[0][0]=='3' or  code.keys()[0][0]=='0' or code.keys()[0][0]=='6':
                    c = code.keys()[0]
                else:
                    c =code.keys()[1]

                

                series={"date":[],"close":[],"change":[]}
                if c=='3' or c=='0':
                    tmp = 'sz'+c
                else:
                    tmp='sh'+c

               

                for stockdaily in self.oriprice[tmp].find({"date": {"$gte": timehundrd,"$lt":timetoday}}).sort("date"):
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


                # realtime 中可能没有获取的股票数据，因此加上一个判断是否存在的句子
                industrydetail=i+"detail"+"+detail" 
                totalnames.append(industrydetail)

                if c in realTime.index:
                    pe = realTime.loc[c].per
                    name = realTime.loc[c].stockname
                    turnoverratio=realTime.loc[c].turnoverratio
                    changepercent=realTime.loc[c].changepercent
                    volume = realTime.loc[c].volume
                    makrket = realTime.loc[c].mktcap
                    dict1={"code":c,"name":name,"pe":pe,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day}
                    self.indestrytimepool[industrydetail].insert(dict1)
                    



                if c in realTime.index:
                    if isinstance(realTime.ix[c].changepercent, np.float) and isinstance(realTime.ix[c].mktcap, np.float):
                        todaydiff =todaydiff+realTime.ix[c].changepercent*realTime.ix[c].mktcap/100
                        summarket =summarket+realTime.ix[c].mktcap
                    if isinstance(realTime.ix[c].per, np.float) and realTime.ix[c].per > 0 and realTime.ix[c].per < 500:
                        totalpe +=realTime.ix[c].per
                        n +=1

            if n == 0:
                averagepe = 0
            else:
                averagepe = totalpe/n;
            
            
            indexname = i+"detail"

            today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
            hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

            indestrydetail = DataFrame(list(self.industryindex[indexname].find().sort("date")))
            names=['id','pe','time','value']
            indestrydetail.columns=names
            indestrydetail.index=indestrydetail.time

            nowvalue = indestrydetail.iloc[-1].value*(1+todaydiff/summarket)


            industry_5day= (nowvalue-indestrydetail.iloc[-6].value)/nowvalue
            industry_10day=(nowvalue-indestrydetail.iloc[-11].value)/nowvalue
            industry_30day=(nowvalue-indestrydetail.iloc[-31].value)/nowvalue
            industry_90day=(nowvalue-indestrydetail.iloc[-91].value)/nowvalue
            
            mydict = {"ape":indestrydetail.iloc[-1].pe,"now":nowvalue,"diff":todaydiff/summarket,"5day":industry_5day,"10day":industry_10day,"30day":industry_30day,"90day":industry_90day}


            # 获取当前时间
            minutetime =time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()))
            industryindexname=i+'+index';
            totalnames.append(industryindexname)
            self.indestrytimepool[industryindexname].insert(mydict)
        
        allindustry = self.indestrytimepool.collection_names()
        #print totalnames
        for con in allindustry:
            if con not in totalnames:
                #print con+" industry out"
                self.indestrytimepool[con].drop()

        allindustry = self.indestrytimepool.collection_names()
        for con in allindustry:
            #print con+" industry in"
            #print con.split('+')[0]
            self.indestrytimepool[con].rename(con.split('+')[0])








            

    ## 数据库连接设置
    def Conn(self):
        
        self.localclient = pymongo.MongoClient(self.localip,self.localport)

        self.remoteclient = pymongo.MongoClient(self.remoteip,self.remoteport)
        
        self.rawstockdata=self.remoteclient.rawstockdata          #存储股票原始数据,按照日进行索引
        
        self.rawindexdata = self.remoteclient.rawindexdata        #存储实时指数数据

        self.realtimepool = self.localclient.basirealtimepool    #存储实时行情数据
        

        self.industry = self.localclient.industry # 存储行业信息
        self.industryindex = self.localclient.industryindex #行业指数信息

        self.indestrytimepool =self.localclient.industryrealtimepool  # 存储行业指数实时行情

        self.oriprice=self.localclient.stockoriginalprice      #日行情数据库
        


    '''
    关闭数据库
    '''
    def Close(self):
        self.remoteclient.close()
        self.localclient.close()


    def DeletePool(self):
        alldatabase=self.realtimepool.collection_names()
        for con in alldatabase:
            #print u"delte "+con
            self.realtimepool[con].drop()

        alldatabase=self.indestrytimepool.collection_names()
        for con in alldatabase:
            #print u"delte "+con
            self.indestrytimepool[con].drop()




if __name__ == '__main__':
    I=MinuteOperation(constants.REMOTEIP,constants.REMOTEPORT,constants.LOCALIP,constants.LOCALPORT)
    I.Conn()
    I.DeletePool()
    while True:
        realTime,realIndex = I.getRealTime()
        I.storagepool(realTime,realIndex)
        I.ComputeRealIndex(realTime)

    I.Close()