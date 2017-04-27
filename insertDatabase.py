 #encoding:utf-8
import sys

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
from mongodb import LoadDataCVS
from pandas import DataFrame
import constants
import math
# add new data and use tushare to update data
class InsertDataCVS:

    basedir=constants.basedir
    stockdata=constants.stockdata
    indexdata=constants.indexdata
    newdata=constants.newdata

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
        self.qfqprice=self.client.stockqfqprice #stock qfq price
        self.hfqprice=self.client.stockhfqprice #stock hfq price


        self.test=self.client.test


    '''
    关闭数据库
    '''
    def Close(self):
        self.client.close()




    '''
    存储并计算前复权数据，使用每天数据的变化量，从后往前推
    '''
    def storageqfq(self):
        alldatabase=self.oriprice.collection_names()
        for code in alldatabase:
            
            if code =='system.indexes':
                continue

            stockdatalen=self.oriprice[code].find().count()
            rows=self.oriprice[code].find().sort("date")
            i=stockdatalen-1

            flag=False
            yesterday=rows[stockdatalen-1]
            while i > 0:
                
                today=yesterday

                yesterday=rows[i-1]

                computeyesterdayclose=today['close']/(1+float(today['change']))
                #print computeyesterdayclose,yesterday['open']/today['open']
                computeopen=yesterday['open']/yesterday['close']*computeyesterdayclose
                computehigh=yesterday['high']/yesterday['close']*computeyesterdayclose
                computelow=yesterday['low']/yesterday['close']*computeyesterdayclose
                
                realyesterday=yesterday['close']


                # 说明数据需要用复权数据重新计算
                if flag==False:
                    if (math.fabs(computeyesterdayclose - realyesterday)+0.0)/(computeyesterdayclose+0.0)>0.05:
                        flag=True
                        series={}
                        series['code']=yesterday['code']
                        series["open"]=computeopen
                        series["high"]=computehigh
                        series["low"]=computelow
                        series["close"]=computeyesterdayclose
                        series["change"]=yesterday['change']
                        series["volume"]=yesterday['volume']
                        series["money"]=yesterday['money']
                        series["traded_market_value"]=yesterday['traded_market_value']
                        series["market_value"]=yesterday['market_value']
                        series["turnover"]=yesterday['turnover']
                        series['date'] =yesterday['date']

                        print(u"股票: "+code+u"从"+str(today['date'])+u" 开始需要进行复权处理,今天的价格为"+str(today['close']))
                        #更新yesterday 的情况，主要是更新价格
                        yesterday=series
                    else:
                        series=yesterday

                else:
                    print(u"股票: "+code+u"进行"+str(today['date'])+u"复权处理,收盘价格为"+str(computeyesterdayclose))
                    series={}
                    series['code']=yesterday['code']
                    series["open"]=computeopen
                    series["high"]=computehigh
                    series["low"]=computelow
                    series["close"]=computeyesterdayclose
                    series["change"]=yesterday['change']
                    series["volume"]=yesterday['volume']
                    series["money"]=yesterday['money']
                    series["traded_market_value"]=yesterday['traded_market_value']
                    series["market_value"]=yesterday['market_value']
                    series["turnover"]=yesterday['turnover']
                    series['date'] =yesterday['date']
                    yesterday=series

                

                i-=1



    '''
    存储每天股票的原始行情数据
    '''
    def storagedaily(self):
        #get the filelist in stockdata
        onlyfiles = [ f for f in listdir(self.stockdata) if isfile(join(self.stockdata,f)) ]
        
        #read from using pandas
        alldatabase=self.oriprice.collection_names()
        for f in onlyfiles:
            df = pd.read_csv(self.stockdata+"/"+f)
            #del those, insert them into another data base
            del df['adjust_price']
            del df['report_type']
            del df['report_date']
            del df['PE_TTM']
            del df['PS_TTM']
            del df['PC_TTM']
            del df['PB']
            del df['adjust_price_f']
            s=f.split('.')
            name = s[0]

            if name in alldatabase:
                print(name+u"2017-2-28之前数据已经存在")
                continue

            #first drop the database if exits
            #self.connection.drop_collection(name)
            print(u"正在插入数据:"+name)
            records = json.loads(df.T.to_json()).values()
            for row in records:
                row['date'] = datetime.datetime.strptime(row['date'], "%Y-%m-%d")


            self.oriprice[name].insert_many(records) 


        # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.newdata) if isfile(join(self.newdata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='i':
                continue

            df = pd.read_csv(self.newdata+"/"+f)
            if len(f) < 12:
                continue

            del df['adjust_price']
            del df['report_type']
            del df['report_date']
            del df['PE_TTM']
            del df['PS_TTM']
            del df['PC_TTM']
            del df['PB']
            del df['adjust_price_f']

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
                series["turnover"]=df.iloc[i].turnover
                series['date'] = datetime.datetime.strptime(df.iloc[i].date, "%Y-%m-%d")
                name=code
                #check exits
                countstock=self.oriprice[name].find({"date":series['date']}).count()
                if countstock ==0:
                    print (u"插入股票"+name+" "+str(series["date"])+u"数据")
                    self.oriprice[name].insert_one(series)
                else:
                    print(u"股票"+name+" "+str(series['date'])+u"已经存在")
                




    '''
    存储指数数据到数据库中
    '''
            
    def storageindex(self):
        #get the filelist
        alldatabase=self.index.collection_names()

        onlyfiles = [ f for f in listdir(self.indexdata) if isfile(join(self.indexdata,f)) ]
        #read from using pandas
        for f in onlyfiles:
            df = pd.read_csv(self.indexdata+"/"+f)
            s=f.split('.')
            name = s[0]
            if name in alldatabase:
                print(u"指数"+name+u" 2017-2-28之前数据已经在数据库中")
                continue
            records = json.loads(df.T.to_json()).values()
            for row in records:
                row['date'] = datetime.datetime.strptime(row['date'], "%Y-%m-%d")
            self.index[name].insert_many(records)


        # get stock data in newdata dir
        onlyfiles = [ f for f in listdir(self.newdata) if isfile(join(self.newdata,f)) ]

        #read from using pandas
        #print self.connection.collection_names()
        for f in onlyfiles:
            fdate=f[0:10]
            kind=f[11]
            if kind=='d':
                continue

            df = pd.read_csv(self.newdata+"/"+f)
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




            
    
    
    #storage stock pool into database
    def storagepool(self):
        #storage zz500
        df=ts.get_zz500s()
        self.pool['zz500'].insert_many(json.loads(df.to_json(orient='records')))
        #hs300
        df=ts.get_hs300s()
        self.pool['hz300'].insert_many(json.loads(df.to_json(orient='records')))
        #zh50
        df=ts.get_sz50s()
        self.pool['sz'].insert_many(json.loads(df.to_json(orient='records')))
        #st
        df=ts.get_st_classified()
        self.pool['st'].insert_many(json.loads(df.to_json(orient='records')))






    def get_data(self):

        in_package_data = range(2002, 2017)
        cur_year = datetime.datetime.now().year
        last_in_package_data = max(in_package_data)


        # download new data
        to_downloads = range(last_in_package_data + 1, cur_year + 1)

        # frist, get ycDefIds params
        response = requests.get(self.YIELD_MAIN_URL)

        matchs = re.search(r'\?ycDefIds=(.*?)\&', response.text)
        ycdefids = matchs.group(1)
        assert (ycdefids is not None)

        fetched_data = []
        for year in to_downloads:
            print('Downloading from ' + self.DONWLOAD_URL % (year, ycdefids))
            response = requests.get(self.DONWLOAD_URL % (year, ycdefids))
            fetched_data.append(BytesIO(response.content))

        # combine all data

        dfs = []

        basedir = os.path.join(os.path.dirname(__file__), "xlsx")

        for i in in_package_data:
            dfs.append(pd.read_excel(os.path.join(basedir, "%d.xlsx" % i)))

        for memfile in fetched_data:
            dfs.append(pd.read_excel(memfile))

        df = pd.concat(dfs)

        return df

    def get_pivot_data(self):

        df = self.get_data()
        return df.pivot(index=u'日期', columns=u'标准期限(年)', values=u'收益率(%)')



    '''
    插入国债数据
    '''

    def insert_zipline_treasure_format(self):
        pivot_data = self.get_pivot_data()

        frame=pivot_data[[0.08,0.25,0.5,1,2,3,5,7,10,20,30]]
        frame['Time Period']=frame.index
        frame['Time Period']=frame['Time Period'].astype('str')
        frame.columns=['1month', '3month','6month', '1year', '2year', '3year', '5year', '7year', '10year', '20year', '30year','Time Period']
        records = json.loads(frame.T.to_json()).values()

        for row in records:
            temp=row['Time Period']
            temp=temp.split('T')[0]
            row['Time Period'] = datetime.datetime.strptime(temp, "%Y-%m-%d")
            countreasure=self.treasure['treasure'].find({"Time Period":row['Time Period']}).count()
            if countreasure ==0:
                print(str(row['Time Period'])+u" 插入数据")
                self.treasure['treasure'].insert_one(row)
            else:
                print(str(row['Time Period'])+u" 的国债数据已经存在,不用重复插入")




    '''
    删除国债数据
    '''
    def Delete_treasure(self):
        print u"删除treasure表"
        self.treasure['treasure'].drop()
        #self.client.drop_collection['treasure']



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


    '''
    删除原始股票池数据
    '''
    def InsertTodayAll(self,name,series):
        #check exits
        countstock=self.test[name].find({"date":series['date']}).count()
        if countstock ==0:
            print (u"插入股票"+name+str(series["date"])+u"数据")
            self.test[name].insert_one(series)
        else:
            print(u"股票"+name+" "+str(series['date'])+u"已经存在")


    def Close(self):
        self.client.close()



    # def storageStockName(self):
    #     totalstock=[]
    #     onlyfiles = [ f for f in listdir(self.stockdata) if isfile(join(self.stockdata,f)) ]
    #     for f in onlyfiles:
    #         s=f.split('.')
    #         name=s[0][2:8]
    #         totalstock.append(name)
            
    #     data = {'codes': totalstock}
    #     frame = DataFrame(data)
        
    #     self.pool['all'].insert_many(json.loads(frame.to_json(orient='records')))
    #     print frame


if __name__ == '__main__':
    I=InsertDataCVS(constants.IP,constants.PORT)
    #l=LoadDataCVS('127.0.0.1',27017)
    I.Conn()
    I.storageqfq()
    I.Close()
    # I.Delete_stock()
    # I.Delete_treasure()
    # # # I.Delete_connection()
    # I.insert_zipline_treasure_format()
    # # I.Close()
    # #I.storageqfq()
    # I.storageindex()
    # I.storagedaily()
    # I.Close()
    #I.storageindex()

    #I.InsertStock("/Users/zhanghan/Downloads/trading-data-push-20170301/2017-03-01 data.csv")
    #print l.getstockdaily('002759','2016-2-21','2016-7-2')
    # l.Conn()
    # l.storagedaily()
    # l.storageindex()
    # l.storagepool()
    # l.storageStockName()
    # l.insert_zipline_treasure_format()

    #l.storageStockName()
    #print l.getstocklist('all')
