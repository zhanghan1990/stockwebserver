# encoding: utf-8
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
        while i <= int(pagenumber):
            #print i,pagenumber
            url='http://q.10jqka.com.cn/gn/detail/field/199112/order/desc/page/'+str(i)+'/ajax/1/code/'+code
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
                    print things[0],things[1]
            
            i+=1



    def start(self,url):
        IndexPage=self.getPage(url)
        industry=self.getContent(IndexPage)
        print industry['name']
        for key in industry.keys():
           # print industry[key]['name']
            # 获取每个industry的详尽代码
            urldetail = 'http://q.10jqka.com.cn/gn/detail/field/199112/order/desc/page/1/ajax/1/code/'+key
            firstpage=self.getPage(urldetail)
            pattern=re.compile('<span class="page_info">1/(.*?)</span>',re.S)
            items = re.findall(pattern,firstpage)
            # 说明只有一页
            if items==[]:
                print "DDDDDD"
            else:
                self.getDetail(key,items[0])
            for item in items:
                print item
                self.getDetail(key,item[0])
            


bdtb = THS()
bdtb.start('http://q.10jqka.com.cn/thshy/')
#bdtb.start('http://q.10jqka.com.cn/gn/')
# from selenium import webdriver
 
# browser = webdriver.Chrome('/Users/zhanghan/Downloads/chromedriver')
# browser.get('http://www.baidu.com/')

