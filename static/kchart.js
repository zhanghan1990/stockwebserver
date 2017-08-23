require.config({
        paths: {
            echarts: '../static',
        }
    });
    
    require(
        [
            'echarts',
            'echarts/chart/k', // 按需加载
        ],
        function (ec) {
            //--- 折柱 ---
            var myChart = ec.init(document.getElementById('main'));
            var myUrl = document.getElementById('url').innerText;
            var myTitle = document.getElementById('title').innerText;
            console.log(myUrl)
           
            // 设置---------------------
            option = {
    tooltip : {
        trigger: 'axis',
        formatter: function (params) {
            var res = params[0].seriesName + ' ' + params[0].name;
            res += '<br/>  开盘 : ' + params[0].value[0] + '  最高 : ' + params[0].value[3];
            res += '<br/>  收盘 : ' + params[0].value[1] + '  最低 : ' + params[0].value[2];
            return res;
        }
    },
    legend: {
        data:[myTitle]
    },
    toolbox: {
        show : true,
        feature : {
            mark : {show: true},
            dataZoom : {show: true},
            dataView : {show: true, readOnly: false},
            restore : {show: true},
            saveAsImage : {show: true}
        }
    },
    dataZoom : {
        show : true,
        realtime: true,
        start : 0,
        end : 50
    },
    xAxis : [
        {
            type : 'category',
            boundaryGap : true,
            axisTick: {onGap:false},
            splitLine: {show:false},
            data : []
        }
    ],
    yAxis : [
        {
            type : 'value',
            scale:true,
            boundaryGap: [0.01, 0.01]
        }
    ],
    series : [
        {
            name:'上证指数',
            type:'k',
            barMaxWidth: 20,
            itemStyle: {
                normal: {
                    color: 'red',           // 阳线填充颜色
                    color0: 'lightgreen',   // 阴线填充颜色
                    lineStyle: {
                        width: 2,
                        color: 'orange',    // 阳线边框颜色
                        color0: 'green'     // 阴线边框颜色
                    }
                },
                emphasis: {
                    color: 'black',         // 阳线填充颜色
                    color0: 'white'         // 阴线填充颜色
                }
            },
            data:[]
        }
    ]
};
            $.ajax({
                cache: false,
                type: "GET",
                url: myUrl, //把表单数据发送到/stock
                data: null, // 发送的数据
                dataType : "json",  //返回数据形式为json
                async: false,
                error: function(request) {
                    alert("发送请求失败！");
                },
                success: function(result) {
                    max = result.date.length;
                    for(i = 0 ; i<max; i++){
                        option.xAxis[0].data.push(result.date[i]);
                        option.series[0].data.push([result.open[i],result.close[i],result.low[i],result.high[i]]);
                    };
                    myChart.setOption(option);
                }
            });
        }
    );