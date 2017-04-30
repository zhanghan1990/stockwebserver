# stockwebserver
## A股API说明

- 获取某只股票从 start 到 end 的行情
```
http://192.168.1.127:5000/stock/sh603429?start=1991-01-01&end=2000-01-01
```
返回json格式数据：
```
{"traded_market_value": 865980000.0, "high": 50.94, "market_value": 3463920000.0, "code": "sh603429", "money": 10092538.0, "volume": 198126.0, "low": 50.94, "date": {"$date": 1486944000000}, "close": 50.94, "_id": {"$oid": "59016e2e1d41c8596906c3bc"}, "open": 50.94, "change": 0.099978, "turnover": 0.011654}
```
