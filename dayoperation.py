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
import numpy

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

        self.specialindex = self.client.specialindex #存储自定义股票池指数
        self.specialpool = self.client.specialpool #存储自定义的股票池



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
        #self.insertDetail(realTime,tusharedata,tushareindex,'399005',smallindexname,smalldetail)
        
        
        # 创业板
        createindexname='create'
        createdetail='createdetail'
        #self.insertDetail(realTime,tusharedata,tushareindex,'399006',createindexname,createdetail)
        




    def insertDetail(self,realTime,tusharedata,tushareindex,indexcode,dbname,dbdetail):
        
        #print fiveday,tenday,thirtyday,nightyday

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
	print df

        ape=0
        up =0
        down =0
        number = 0

        fivedata,tendata,thirtydata,nightydata=self.getindexdata(indexcode)

        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

        #获取指数 过去一段时间的特性
        indexhistorydata=ts.get_k_data(code=indexcode,index=True,start=hundredyday,end=today)
        #print indexhistorydata
        index_5day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-4].close)/indexhistorydata.iloc[-1].close*100
        index_10day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-7].close)/indexhistorydata.iloc[-1].close*100
        index_30day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-21].close)/indexhistorydata.iloc[-1].close*100
        index_90day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-64].close)/indexhistorydata.iloc[-1].close*100



        for i in range(0,len(df)):
            code=df.iloc[i].code

            if code[0]=='3' or code[0]=='0':
                tmp = 'sz'+code
            elif code[0]=='6':
                tmp='sh'+code
            else:
                tmp = code


            dict1=self.getstockdetail(realTime,tusharedata,fivedata,tendata,thirtydata,nightydata,tmp)
            if dict1 == None:
                continue
            self.realtimepool[dbdetail].insert(dict1)

            if realTime.loc[tmp].PE_TTM >0 and realTime.loc[tmp].PE_TTM < 1000:
                ape+=realTime.loc[tmp].PE_TTM
                number +=1

            
            if float(dict1['change']) >0:
                up+=1
            if float(dict1['change']) <0:
                down+=1

            
         
        ape /=number


        mydict = {"ape":ape,"now":tushareindex.ix[indexcode].close,"diff":tushareindex.ix[indexcode].change,"5day":index_5day,"10day":index_10day,"30day":index_30day,"90day":index_90day,"up":up,"down":down}
        #print mydict
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
        allindustry=self.industry.collection_names()  

        totalnames=[]
        
        fivedata,tendata,thirtydata,nightydata=self.getindexdata('000001')


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
                if code.keys()[0][0]=='3' or  code.keys()[0][0]=='0' or code.keys()[0][0]=='6':
                    c = code.keys()[0]
                else:
                    c =code.keys()[1]


                if c[0]=='3' or c[0]=='0':
                    tmpcode= 'sz'+c
                elif c[0]=='6':
                    tmpcode='sh'+c
                else:
                    tmpcode=c

               
                tmp = tusharedata.loc[tmpcode[2:8]].code
                
                dict1=self.getstockdetail(realTime,tusharedata,fivedata,tendata,thirtydata,nightydata,tmpcode)
                if dict1 == None:
                    continue
                self.indestrytimepool[industrydetail].insert(dict1)

                todaydiff +=float(dict1['change'])*float(dict1['marketcap'])/100
                summarket +=float(dict1['marketcap'])

                if float(dict1['change']) >0:
                     up+=1
                if float(dict1['change']) <0:
                    down+=1

                
                if realTime.loc[tmpcode].PE_TTM > 0 and realTime.loc[tmpcode].PE_TTM < 1000:
                    pen+=1
                    totalpe +=realTime.loc[tmpcode].PE_TTM
                if realTime.loc[tmpcode].PB > 0:
                    pbn+=1
                    totalpb +=realTime.loc[tmpcode].PB
                if realTime.loc[tmpcode].PC_TTM > 0:
                    pcn+=1
                    totalpc +=realTime.loc[tmpcode].PC_TTM
                if realTime.loc[tmpcode].PS_TTM > 0:
                    psn+=1
                    totalps +=realTime.loc[tmpcode].PS_TTM

                                
            averagepe=totalpe/pen
            averagepb=totalpb/pbn
            averageps=totalps/psn
            averagepc=totalpc/pcn

            #print averagepe,averagepb,averageps,averagepc

            indexname = i+"detail"

            today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
            hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")


            #获取指数 过去一段时间的特性
            indexhistorydata=ts.get_k_data(code='000001',index=True,start=hundredyday,end=today)
            #print indexhistorydata
            index_5day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-4].close)/indexhistorydata.iloc[-1].close*100
            index_10day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-7].close)/indexhistorydata.iloc[-1].close*100
            index_30day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-21].close)/indexhistorydata.iloc[-1].close*100
            index_90day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-64].close)/indexhistorydata.iloc[-1].close*100


            fiveday = indexhistorydata.iloc[-4].date
            tenday = indexhistorydata.iloc[-7].date
            thirtyday = indexhistorydata.iloc[-21].date
            nightyday = indexhistorydata.iloc[-64].date



            indestrydetail = DataFrame(list(self.industryindex[indexname].find().sort("date")))
            names=['id','pe','time','value']
            indestrydetail.columns=names
            indestrydetail.index=indestrydetail.time

            nowvalue = indestrydetail.iloc[-1].value*(1+todaydiff/summarket)


            industry_5day= (nowvalue-indestrydetail.ix[fiveday].value)/nowvalue*100
            industry_10day=(nowvalue-indestrydetail.ix[tenday].value)/nowvalue*100
            industry_30day=(nowvalue-indestrydetail.ix[thirtyday].value)/nowvalue*100
            industry_90day=(nowvalue-indestrydetail.ix[nightyday].value)/nowvalue*100
            
            mydict = {"ape":averagepe,"apb":averagepb,"aps":averageps,"apc":averagepc,"up":up,"down":down,"now":nowvalue,"diff":todaydiff/summarket*100,"5day":industry_5day,"10day":industry_10day,"30day":industry_30day,"90day":industry_90day}
            self.indestrytimepool[i].insert(mydict)


    def getindexdata(self,indexcode):
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

        #获取指数 过去一段时间的特性
        indexhistorydata=ts.get_k_data(code=indexcode,index=True,start=hundredyday,end=today)
        #print indexhistorydata
        index_5day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-4].close)/indexhistorydata.iloc[-1].close*100
        index_10day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-7].close)/indexhistorydata.iloc[-1].close*100
        index_30day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-21].close)/indexhistorydata.iloc[-1].close*100
        index_90day=(indexhistorydata.iloc[-1].close-indexhistorydata.iloc[-64].close)/indexhistorydata.iloc[-1].close*100


        fiveday = indexhistorydata.iloc[-4].date
        tenday = indexhistorydata.iloc[-7].date
        thirtyday = indexhistorydata.iloc[-21].date
        nightyday = indexhistorydata.iloc[-64].date



        fivedata= DataFrame(list(self.pricetime[fiveday].find()))
        fivedata.index=fivedata.code

        tendata=DataFrame(list(self.pricetime[tenday].find()))
        tendata.index= tendata.code

        thirtydata=DataFrame(list(self.pricetime[thirtyday].find()))
        thirtydata.index=thirtydata.code

        nightydata = DataFrame(list(self.pricetime[nightyday].find()))
        nightydata.index = nightydata.code

        return fivedata,tendata,thirtydata,nightydata


        
    def getstockdetail(self,realTime,tusharedata,fivedata,tendata,thirtydata,nightydata,tmpcode):
        if  tmpcode not in realTime.index:
             return None
        
        if tmpcode not in fivedata.index:
            stock_5day=numpy.nan
        else:
            stock_5day=(realTime.ix[tmpcode].adjust_price-fivedata.ix[tmpcode].adjust_price)/realTime.ix[tmpcode].adjust_price*100
        
        if tmpcode not in tendata.index:
            stock_10day = numpy.nan
        else:
            stock_10day=(realTime.ix[tmpcode].adjust_price-tendata.ix[tmpcode].adjust_price)/realTime.ix[tmpcode].adjust_price*100
        
        if tmpcode not in thirtydata.index:
            stock_30day = numpy.nan
        else:
            stock_30day=(realTime.ix[tmpcode].adjust_price-thirtydata.ix[tmpcode].adjust_price)/realTime.ix[tmpcode].adjust_price*100

        if tmpcode not in nightydata.index:
            stock_90day = numpy.nan
        else:   
            stock_90day=(realTime.ix[tmpcode].adjust_price-nightydata.ix[tmpcode].adjust_price)/realTime.ix[tmpcode].adjust_price*100
                        
            
        pe = realTime.loc[tmpcode].PE_TTM
        
        if tmpcode[2:8] not in tusharedata.index:
            
            return None

        name = tusharedata.loc[tmpcode[2:8]].stockname
        turnoverratio=float(realTime.loc[tmpcode].turnover)*100
        changepercent=float(realTime.loc[tmpcode].change)*100
        volume = realTime.loc[tmpcode].volume/10000
        makrket = float(realTime.loc[tmpcode].traded_market_value)/100000000
        pb = realTime.loc[tmpcode].PB
        pc = realTime.loc[tmpcode].PC_TTM
        ps = realTime.loc[tmpcode].PS_TTM

                    
        dict1={"code":tmpcode[2:8],"name":name,"pe":pe,"pb":pb,"pc":pc,"ps":ps,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day}

        return dict1

    def computeSpecial(self,realTime,tusharedata):
        nightycodes={"000333","600028","600104","600887","601288",\
        "601006","601088","601088","600690","600309","600741","600585",\
        "600660","600809","600018","601628","600518","600029","000568",\
        "601988","000858","600023","601398","600795","600703","600900","600009",\
        "600196","601607","601111","300072","601933","600688",\
        "002475","600297","002027","600886","601366","000069","000423",\
        "600019","601588","002271","000418","600436","002543","000063","600276","600872"\
        "600132","600036","601601","603288","601225","002032","601939","300003","002508","000963",\
        "002008","002415","000538","601318"}
        fivedata,tendata,thirtydata,nightydata=self.getindexdata('000001')
        
        for code in nightycodes:
            if code[0]=='3' or code[0]=='0':
                tmp = 'sz'+code
            elif code[0]=='6':
                tmp='sh'+code
            
            dict1=self.getstockdetail(realTime,tusharedata,fivedata,tendata,thirtydata,nightydata,tmp)
            print dict1

            

        
            
if __name__ == '__main__':
    I=DayOperation(constants.IP,constants.PORT)
    I.Conn()
    #I.storagedayprice()
   #I.storagebyTime()
    #I.ComputeIndustryIndexDay()
    I.DeletePool()
    realTime,tusharedata,tushareindex=I.getReal()
    
    I.getToday(realTime,tusharedata,tushareindex)
    I.ComputeRealIndex(realTime,tusharedata,tushareindex)
    #I.computeSpecial(realTime,tusharedata)
    I.Close()
