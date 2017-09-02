#encoding:utf-8
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
from zipline import TradingAlgorithm
from zipline.api import order, sid,get_datetime
from zipline.data.loader import load_data
from zipline.api import order_target, record, symbol, history, add_history,symbol,set_commission,order_percent,set_long_only,get_open_orders
from zipline.finance.commission import OrderCost
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False



# loading the data
input_data = load_data(
    stockList=['002057'],
    start="2015-11-04",
    end="2016-01-16"
)

f=open('test','r')
code=f.read()
f.close()
# capital_base is the base value of capital
#
#algo = TradingAlgorithm(initialize=initialize, handle_data=handle_data,capital_base=10000,benchmark='sz399004')
algo = TradingAlgorithm(script=code,capital_base=10000,benchmark='sz399004')
#print input_data
#api: print all the api function
#print algo.all_api_methods()
results = algo.run(input_data)
print results
#print results['benchmark_period_return'],results['portfolio_value']
#analyze(results=results)
