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
爬取同花顺的行业和概念信息
'''
import urllib
import urllib2
import re

class Tool:
    removeImg=re.compile('<img.*?>| {7}')
    removeAddr=re.compile('<a.*?>|</a>')
    replaceLine = re.compile('<tr>|<div>|</div>|</p>')
    replaceTD= re.compile('<td>')
    replacePara = re.compile('<p.*?>')
    replaceBR = re.compile('<br><br>|<br>')
    removeExtraTag = re.compile('<.*?>')

    def Replace(self,x):
        x=re.sub(self.removeImg,"",x)
        x = re.sub(self.removeAddr,"",x)
        x = re.sub(self.replaceLine,"\n",x)
        x = re.sub(self.replaceTD,"\t",x)
        x = re.sub(self.replacePara,"\n    ",x)
        x = re.sub(self.replaceBR,"\n",x)
        x = re.sub(self.removeExtraTag,"",x)
        return x.strip()

class THS:
    def __init__(self):
        self.tool=Tool()

    def getPage(self,url):
        try:
            request=urllib2.Request(url)
            response=urllib2.urlopen(request)
            return response.read().decode('gbk','ignore')
        except urllib2.URLError,e:
            if hasattr(e,"reason"):
                print u"网络连接存在异常",e.reason
                return None
    def getTitle(self,page):
        #注意，这里re.compile需要的参数是一个string，而不是一个list
        pattern=re.compile('<h3 class="core_title_txt pull-left text-overflow.*?>(.*?)</h3>',re.S)
        result=re.search(pattern,page)
        if result:
            return result.group(1).strip()
        else:
            return None

    def getPageNumber(self,page):
        pattern=re.compile('<li class="l_reply_num.*?>.*?<span class="red">(.*?)</span>',re.S)
        result=re.search(pattern,page)
        if result:
            return result.group(1).strip()
        else:
            return None

    def setFileTitle(self,title):
        if title is not None:
            self.file=open(title+".txt","w+")
        else:
            self.file = open(self.defaultTitle + ".txt","w+")

    def writeData(self,contents):
        for item in contents:
            if self.floorTag == '1':
                floorLine = "\n" + str(self.floor) + u"-----------------------------------------------------------------------------------------\n"
                self.file.write(floorLine)
            self.file.write(item)
            self.floor += 1

    def getContent(self,page):
        industry={}
        pattern=re.compile('<div class="cate_items">(.*?)</div>',re.S)
        items = re.findall(pattern,page)
        contents=[]
        for item in items:
            pattern2=re.compile('<a href="(.*?)" target="_blank">(.*?)</a>',re.S)
            urls = re.findall(pattern2,item)
            for url in urls:
                indestrycode=url[0].split("code")[1].split("/")[1].split("/")[0]
                industryurl=url[0]
                industryname=url[1]
                #print indestrycode,industryurl,industryname
                industry[indestrycode]={"name":industryname,"url":industryurl}

        return industry

    def getDetail(self,code,pagenumber):
        i = 1
        ret=[]
        while i <= int(pagenumber):
            #print i,pagenumber
            url='http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/'+str(i)+'/ajax/1/code/'+code
            print url
            detail = self.getPage(url)
            pattern=re.compile('<tbody>(.*?)</tbody>',re.S)
            tables = re.findall(pattern,detail)
            pattern2=re.compile('<tr>(.*?)</tr>',re.S)
            row = re.findall(pattern2,tables[0])
            for r in row:
                pattern3='.*?target="_blank">(.*?)</a></td>'
                things = re.findall(pattern3,r)
                if len(things)>=2:
                    ret.append({things[0]:things[1]})
            
            i+=1

        return ret


    def start(self,url):
        IndexPage=self.getPage(url)
        industry=self.getContent(IndexPage)

        for key in industry.keys():
            # 获取每个industry的详尽代码
            urldetail = 'http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/1/ajax/1/code/'+key
            firstpage=self.getPage(urldetail)
            pattern=re.compile('<span class="page_info">1/(.*?)</span>',re.S)
            items = re.findall(pattern,firstpage)
            print items
            for item in items:
                #print item
                self.getDetail(key,item[0])
            



# add new data and use tushare to update data
class InsertDataCVS:

    basedir=constants.basedir
    stockdata=constants.stockdata
    indexdata=constants.indexdata
    newdata=constants.newdata

    remoteip = constants.REMOTEIP
    remoteport = constants.REMOTEPORT

    # 国债数据网址和日期 
    in_package_data = range(2002, 2017)
    DONWLOAD_URL = "http://yield.chinabond.com.cn/cbweb-mn/yc/downYearBzqx?year=%s&&wrjxCBFlag=0&&zblx=txy&ycDefId=%s"
    YIELD_MAIN_URL = 'http://yield.chinabond.com.cn/cbweb-mn/yield_main'



    def __init__(self,Ip,port):
        self.ip=Ip
        self.port=port

    

    ## 数据库连接设置
    def Conn(self):
        self.client = pymongo.MongoClient(self.ip,self.port)
        self.connection=self.client.stock #storage stock information
        self.index=self.client.index #storage index
        self.pool=self.client.pool  #storate pool
        
        self.treasure=self.client.treasure
        self.oriprice=self.client.stockoriginalprice #stock original price
        self.realtimepool = self.client.basirealtimepool #存储实时行情数据
        self.basic = self.client.basic # 存储上市公司基本数据
        self.report = self.client.report #存储上市公司业绩报告
        self.industry = self.client.industry # 存储行业信息
        self.industryindex = self.client.industryindex #行业指数信息
        self.concept = self.client.concept #存储概念信息
        self.conceptIndex =  self.client.conceptIndex #行业概念指数
        self.pricetime = self.client.pricetime     #按照时间进行存储
        self.test=self.client.test



        self.remoteclient = pymongo.MongoClient(self.remoteip,self.remoteport)
        self.rawstockdata=self.remoteclient.rawstockdata          #存储股票原始数据,按照日进行索引
        self.rawindexdata = self.remoteclient.rawindexdata        #存储实时指数数据




    '''
    关闭数据库
    '''
    def Close(self):
        self.client.close()


    #计算一次，抓同花顺行业信息
    def InsertIndestry(self):
        
        alldatabase=self.industry.collection_names()
        for con in alldatabase:
            print u"删除"+con
            self.industry[con].drop()


        industryclassified={}
        bdtb = THS()
        IndexPage=bdtb.getPage('http://q.10jqka.com.cn/thshy/')
        industry=bdtb.getContent(IndexPage)
        #print IndexPage

        for key in industry.keys():
            # 获取每个industry的详尽代码
            urldetail = 'http://q.10jqka.com.cn/thshy/detail/field/199112/order/desc/page/1/ajax/1/code/'+key
            firstpage=bdtb.getPage(urldetail)
            pattern=re.compile('<span class="page_info">1/(.*?)</span>',re.S)
            items = re.findall(pattern,firstpage)
            #print items
            for item in items:
                #print item
                print industry[key]['name']
                industryclassified[industry[key]['name']]=bdtb.getDetail(key,item[0])

        for key in industryclassified.keys():
            self.industry[key].drop()
            self.industry[key].insert(industryclassified[key])

        # for i in range(0,length):
        #     d = pd.iloc[i]
        #     if industryclassified.has_key(d.c_name):
        #         .append({d.code:d.name})
        #     else:
        #         industryclassified[d.c_name]=[{d.code:d.name}]

        # for key in industryclassified.keys():
        #     self.industry[key].drop()
        #     self.industry[key].insert(industryclassified[key])


    #每天计算一次，计算概念的指数
    def ComputeRealIndex(self):
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

         #获取指数 过去一段时间的特性
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")

                
        timehundrd = datetime.datetime.strptime(hundredyday, "%Y-%m-%d")
        timetoday =  datetime.datetime.strptime(today, "%Y-%m-%d")
        indexhistorydata=ts.get_k_data(code='000001',index=True,start=hundredyday,end=today)
        
        
        
        # realIndex =  DataFrame(list(self.rawindexdata[dbname].find()))
        # names=['id','amount','change','close','code','high','low','indexname','open','preclose','volume']
        # realIndex.columns = names
        # realIndex.index=realIndex.code




        series = {}
       #每个指数初始化为1000
        allindustry=self.industry.collection_names()  

        for i in allindustry:
            indexstart = 1000
            codes = self.industry[i].find()
            listcodes=[]
            for code in codes:
                listcodes.append(code)
            
            todaydiff = 0
            summarket = 0
            totalpe = 0
            n = 0
            indexname = i+"detail"


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
                if c in realTime.index:
                    pe = realTime.loc[c].per
                    name = realTime.loc[c].stockname
                    turnoverratio=realTime.loc[c].turnoverratio
                    changepercent=realTime.loc[c].changepercent
                    volume = realTime.loc[c].volume
                    makrket = realTime.loc[c].mktcap
                    dict1={"code":c,"name":name,"pe":pe,"change":changepercent,"turnoverratio":turnoverratio,"volume":volume,"marketcap":makrket,"5day":stock_5day,"10day":stock_10day,"30day":stock_30day,"90day":stock_90day}
                    print i,dict1




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
            
            
            

            #print todaydiff,summarket,todaydiff/summarket
            #dict1={"value":todaydiff/summarket,"pe":averagepe}
            today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
            hundredyday=(datetime.datetime.now()-datetime.timedelta(days=150)).strftime("%Y-%m-%d")


            


            indestrydetail = DataFrame(list(self.industryindex[indexname].find().sort("date")))
            names=['id','pe','time','value']
            indestrydetail.columns=names
            indestrydetail.index=indestrydetail.time

            #print indestrydetail
            nowvalue = indestrydetail.iloc[-1].value*(1+todaydiff/summarket)


            industry_5day= (nowvalue-indestrydetail.iloc[-6].value)/nowvalue
            industry_10day=(nowvalue-indestrydetail.iloc[-11].value)/nowvalue
            industry_30day=(nowvalue-indestrydetail.iloc[-31].value)/nowvalue
            industry_90day=(nowvalue-indestrydetail.iloc[-91].value)/nowvalue

            
            mydict = {"ape":indestrydetail.iloc[-1].pe,"now":nowvalue,"diff":todaydiff/summarket,"5day":industry_5day,"10day":industry_10day,"30day":industry_30day,"90day":industry_90day}




   

                








        

    #每天执行一次
    #获取上市公司的基本情况数据，并且存储到mongodb
    def storestockBasic(self):
        basic=ts.get_stock_basics()
        # 首先删除之前的数据
        alldatabase=self.basic.collection_names()
        for con in alldatabase:
            if con =="system.indexes":
                continue
            print u"删除"+con
            self.basic[con].drop()
        
        self.basic['basic'].insert_many(json.loads(basic.to_json(orient='records')))



    # 每天执行一次
    # 按年度、季度获取业绩报表数据
    # 数据表名称规则 year-month
    def storereportdata(self,startyear):
        
        report = self.report.collection_names()
        for r in report:
            print u"删除"+r
            self.report[r].drop()
        
        
        x = datetime.datetime.now()
        nowyear  = int(x.year)
        nowmonth = int(x.month)

        #print nowmonth,nowyear,startyear,startmonth
        
        for year in range(startyear,nowyear+1):
            if year != nowyear+1:
                for month in range(1,5):
                    name=str(year)+"-"+str(month)
                    print u"正在获取数据"+str(year)+"-"+str(month)
                    data=ts.get_profit_data(year,month)
                    self.report[name].insert_many(json.loads(data.to_json(orient='records')))
            else:
                session = nowmonth/4
                for month in range(1,session):
                     name=str(year)+"-"+str(month)
                     print u"正在获取数据"+str(year)+"-"+str(month)
                     data=ts.get_profit_data(year,month)
                     self.report[name].insert_many(json.loads(data.to_json(orient='records')))
           
            




                



    '''
    删除股票池数据
    '''
    def Delete_pool(self):
        alldatabase=self.pool.collection_names()
        for con in alldatabase:
            print u"删除"+con
            self.connection[con].drop()






    '''
    删除原始股票池数据
    '''
    def Delete_stock(self):
        # self.oriprice.drop_connection['stockoriginalprice']
        alldatabase=self.oriprice.collection_names()
        for con in alldatabase:
            if con =="system.indexes":
                continue
            print u"删除"+con
            self.oriprice[con].drop()





    '''
    删除指数数据
    '''
    def Delete_index(self):
        alldatabase=self.index.collection_names()
        for con in alldatabase:
            if con =="system.indexes":
                continue
            print u"删除"+con
            self.index[con].drop()



    def Close(self):
        self.client.close()



if __name__ == '__main__':
    I=InsertDataCVS(constants.IP,constants.PORT)
    I.Conn()
    I.ComputeRealIndex()
    #I.InsertIndestry()
    #I.ComputeIndustryIndexDay()
    #I.computeIndestryIndex()
    #I.storagepool()
    #I.storestockBasic()
    #I.storereportdata(2016)
    I.Close()