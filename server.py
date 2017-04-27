#encoding:utf-8
from flask import Flask,request
from flask import jsonify
from flask import json
import json
from flask.ext.pymongo import PyMongo
import datetime
import time
from zipline.data import mongodb
from bson import json_util
app = Flask(__name__)
app.config['MONGO_HOST'] = '192.168.1.127'
app.config['MONGO_PORT'] = 27017
mongo = PyMongo(app)


def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%Y-%m-%d")
        return True
    except:
        return False
def toJson(data):
	return json.dumps(data, default=json_util.default)



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
	data1=mongo.db.client.stock[stockname].find()
	json_results = []

	for result in data1:
		json_results.append(result)
	return toJson(json_results)


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)