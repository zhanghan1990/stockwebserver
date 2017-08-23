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
class MinuteData:
    

    def __init__(self,Ip,port):
        self.ip=Ip
        self.port=port



    ## 数据库连接设置
    def Conn(self):
        
        self.client = pymongo.MongoClient(self.ip,self.port)
        
        self.rawstockdata=self.client.rawstockdata          #存储股票原始数据,按照日进行索引
        
        self.rawindexdata = self.client.rawindexdata        #存储实时指数数据


    '''
    关闭数据库
    '''
    def Close(self):
        self.client.close()


    #每分钟执行一次
    #获得实时行情数据，存储到mongodb中
    def storageraw(self):
        # 非交易时间，不进行数据采集

        #周六，周日
        dayOfWeek = datetime.datetime.now().weekday()
        print "week "+str(dayOfWeek)
        if dayOfWeek==5 or dayOfWeek==6:
            print "it is"+str(dayOfWeek)+"now"
            return 
        minutetime =time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()))
        #非交易时间
        hour = float(time.strftime("%H", time.localtime(time.time())))
        minuite=float(time.strftime("%M", time.localtime(time.time())))
        print str(hour)+":"+str(minuite)
        if hour < 9 or hour > 15 or hour==12:
            print "1"+str(minuite)
            return 
        elif hour ==11 and minuite > 30:
            print "2"+str(minuite)
            return
        elif hour ==9 and minuite <30:
            print "3"+str(minuite)
            return 
        
        print minutetime
        print u"insert stock....."
        stocktoday=ts.get_today_all()
        #插入股票数据
        self.rawstockdata[minutetime].insert_many(json.loads(stocktoday.to_json(orient='records')))
        indextoday = ts.get_index()
        #插入指数数据
        print "insert index....."
        self.rawindexdata[minutetime].insert_many(json.loads(indextoday.to_json(orient='records')))

if __name__ == '__main__':
    #0-6
    # dayOfWeek = datetime.datetime.now().weekday()
    # hour = time.strftime("%H", time.localtime(time.time()))
    # minuite=time.strftime("%M", time.localtime(time.time()))
    
    # print hour,minuite
    # print dayOfWeek

    I=MinuteData(constants.IP,constants.PORT)
    I.Conn()
    while True:
        I.storageraw()
        time.sleep(5)

    I.Close()