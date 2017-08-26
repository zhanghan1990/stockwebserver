
        $(document).ready(function(){
            var tableObject = $('#tableSort');//获取id为tableSort的table对象
            var tbHead = tableObject.children('thead');//获取table对象下的thead
            var tbHeadTh = tbHead.find('tr th');//获取thead下的tr下的th
            var tbBody = tableObject.children('tbody');//获取table对象下的tbody
            var tbBodyTr = tbBody.find('tr');//获取tbody下的tr
            var sortIndex = -1; //初始化索引
            tbHeadTh.each(function() {//遍历thead的tr下的th
                var thisIndex = tbHeadTh.index($(this));//获取th所在的列号
                //鼠标移除和聚焦的效果，不重要
                $(this).mouseover(function(){ //鼠标移开事件
                    tbBodyTr.each(function(){//编列tbody下的tr
                        var tds = $(this).find("td");//获取列号为参数index的td对象集合
                        $(tds[thisIndex]).addClass("hover");
                    });
                }).mouseout(function(){ //鼠标聚焦时间
                    tbBodyTr.each(function(){
                        var tds = $(this).find("td");
                        $(tds[thisIndex]).removeClass("hover");
                    });
                });
 
                $(this).click(function() {  //鼠标点击事件
                    var dataType = $(this).attr("type"); //获取当前点击列的 type
                    checkColumnValue(thisIndex, dataType); //对当前点击的列进行排序 （当前索引，排序类型）
                });
            });
 
            //显示效果，不重要
            $("tbody tr").removeClass();//先移除tbody下tr的所有css类
            $("tbody tr").mouseover(function(){
                $(this).addClass("hover");
            }).mouseout(function(){
                $(this).removeClass("hover");
            });
 
            //对表格排序
            function checkColumnValue(index, type) {
                var trsValue = new Array();  //创建一个新的列表
                tbBodyTr.each(function() { //遍历所有的tr标签
                    var tds = $(this).find('td');//查找所有的 td 标签
                    //将当前的点击列 push 到一个新的列表中
                    //包括 当前行的 类型、当前索引的 值，和当前行的值
                    trsValue.push(type + ".separator" + $(tds[index]).html() + ".separator" + $(this).html());
                    $(this).html("");//清空当前列
                });
                var len = trsValue.length;//获取所有要拍戏的列的长度
                if(index == sortIndex){//sortIndex =-1
                    trsValue.reverse();
                } else {
                    for(var i = 0; i < len; i++){//遍历所有的行
                        type = trsValue[i].split(".separator")[0];// 获取要排序的类型
                        for(var j = i + 1; j < len; j++){
                            value1 = trsValue[i].split(".separator")[1].split(">")[1].split("<")[0];//当前值
                            value2 = trsValue[j].split(".separator")[1].split(">")[1].split("<")[0];//当前值的下一个
                            
                            // if(type == "number"){
                            //     //js 三元运算  如果 values1 等于 '' （空） 那么，该值就为0，否则 改值为当前值
                            //     value1 = value1 == "" ? 0 : value1;
                            //     value2 = value2 == "" ? 0 : value2;
                            //     //parseFloat() 函数可解析一个字符串，并返回一个浮点数。
                            //     //该函数指定字符串中的首个字符是否是数字。如果是，则对字符串进行解析，直到到达数字的末端为止，然后以数字返回该数字，而不是作为字符串。
                            //     //如果字符串的第一个字符不能被转换为数字，那么 parseFloat() 会返回 NaN。
                            //     if(parseFloat(value1) > parseFloat(value2)){//如果当前值 大于 下一个值
                            //         var temp = trsValue[j];//那么就记住 大 的那个值
                            //         trsValue[j] = trsValue[i];
                            //         trsValue[i] = temp;
                            //     }
                            // } else if(type == "ip"){
                            //     if(ip2int(value1) > ip2int(value2)){
                            //         var temp = trsValue[j];
                            //         trsValue[j] = trsValue[i];
                            //         trsValue[i] = temp;
                            //     }
                            // } else {
                                //JavaScript localeCompare() 方法 用本地特定的顺序来比较两个字符串。
                                //说明比较结果的数字。
                                // 如果 stringObject 小于 target，则 localeCompare() 返回小于 0 的数。
                                // 如果 stringObject 大于 target，则该方法返回大于 0 的数。
                                // 如果两个字符串相等，或根据本地排序规则没有区别，该方法返回 0。



                                if(parseFloat(value1) > parseFloat(value2)){//如果当前值 大于 下一个值
                                    var temp = trsValue[j]; //那么就记住 大 的那个值
                                    trsValue[j] = trsValue[i];
                                    trsValue[i] = temp;
                                }
                          //  }
                        }
                    }
                }
                for(var i = 0; i < len; i++){
                    //将排序完的 值 插入到 表格中
                    //:eq(index) 匹配一个给定索引值的元素
                    $("tbody tr:eq(" + i + ")").html(trsValue[i].split(".separator")[2]);
                    //console.log($("tbody tr:eq(" + i + ")").html())
                }
                sortIndex = index;
            }
            //IP转成整型 ？？？？？
            function ip2int(ip){
                var num = 0;
                ip = ip.split(".");
                //Number() 函数把对象的值转换为数字。
                num = Number(ip[0]) * 256 * 256 * 256 + Number(ip[1]) * 256 * 256 + Number(ip[2]) * 256 + Number(ip[3]);
                return num;
            }
            })





            $(document).ready(function(){
                var tableObject = $('#tableSort2');//获取id为tableSort的table对象
                var tbHead = tableObject.children('thead');//获取table对象下的thead
                var tbHeadTh = tbHead.find('tr th');//获取thead下的tr下的th
                var tbBody = tableObject.children('tbody');//获取table对象下的tbody
                var tbBodyTr = tbBody.find('tr');//获取tbody下的tr
                var sortIndex = -1; //初始化索引
                tbHeadTh.each(function() {//遍历thead的tr下的th
                    var thisIndex = tbHeadTh.index($(this));//获取th所在的列号
                    //鼠标移除和聚焦的效果，不重要
                    $(this).mouseover(function(){ //鼠标移开事件
                        tbBodyTr.each(function(){//编列tbody下的tr
                            var tds = $(this).find("td");//获取列号为参数index的td对象集合
                            $(tds[thisIndex]).addClass("hover");
                        });
                    }).mouseout(function(){ //鼠标聚焦时间
                        tbBodyTr.each(function(){
                            var tds = $(this).find("td");
                            $(tds[thisIndex]).removeClass("hover");
                        });
                    });
     
                    $(this).click(function() {  //鼠标点击事件
                        var dataType = $(this).attr("type"); //获取当前点击列的 type
                        checkColumnValue(thisIndex, dataType); //对当前点击的列进行排序 （当前索引，排序类型）
                    });
                });
     
                //显示效果，不重要
                $("tbody tr").removeClass();//先移除tbody下tr的所有css类
                $("tbody tr").mouseover(function(){
                    $(this).addClass("hover");
                }).mouseout(function(){
                    $(this).removeClass("hover");
                });
     
                //对表格排序
                function checkColumnValue(index, type) {
                    var trsValue = new Array();  //创建一个新的列表
                    tbBodyTr.each(function() { //遍历所有的tr标签
                        var tds = $(this).find('td');//查找所有的 td 标签
                        //将当前的点击列 push 到一个新的列表中
                        //包括 当前行的 类型、当前索引的 值，和当前行的值
                        trsValue.push(type + ".separator" + $(tds[index]).html() + ".separator" + $(this).html());
                        $(this).html("");//清空当前列
                    });
                    var len = trsValue.length;//获取所有要拍戏的列的长度
                    if(index == sortIndex){//sortIndex =-1
                        trsValue.reverse();
                    } else {
                        for(var i = 0; i < len; i++){//遍历所有的行
                            type = trsValue[i].split(".separator")[0];// 获取要排序的类型
                            for(var j = i + 1; j < len; j++){
                                value1 = trsValue[i].split(".separator")[1].split(">")[1].split("<")[0];//当前值
                                value2 = trsValue[j].split(".separator")[1].split(">")[1].split("<")[0];//当前值的下一个
                                
                                // if(type == "number"){
                                //     //js 三元运算  如果 values1 等于 '' （空） 那么，该值就为0，否则 改值为当前值
                                //     value1 = value1 == "" ? 0 : value1;
                                //     value2 = value2 == "" ? 0 : value2;
                                //     //parseFloat() 函数可解析一个字符串，并返回一个浮点数。
                                //     //该函数指定字符串中的首个字符是否是数字。如果是，则对字符串进行解析，直到到达数字的末端为止，然后以数字返回该数字，而不是作为字符串。
                                //     //如果字符串的第一个字符不能被转换为数字，那么 parseFloat() 会返回 NaN。
                                //     if(parseFloat(value1) > parseFloat(value2)){//如果当前值 大于 下一个值
                                //         var temp = trsValue[j];//那么就记住 大 的那个值
                                //         trsValue[j] = trsValue[i];
                                //         trsValue[i] = temp;
                                //     }
                                // } else if(type == "ip"){
                                //     if(ip2int(value1) > ip2int(value2)){
                                //         var temp = trsValue[j];
                                //         trsValue[j] = trsValue[i];
                                //         trsValue[i] = temp;
                                //     }
                                // } else {
                                    //JavaScript localeCompare() 方法 用本地特定的顺序来比较两个字符串。
                                    //说明比较结果的数字。
                                    // 如果 stringObject 小于 target，则 localeCompare() 返回小于 0 的数。
                                    // 如果 stringObject 大于 target，则该方法返回大于 0 的数。
                                    // 如果两个字符串相等，或根据本地排序规则没有区别，该方法返回 0。
    
    
    
                                    if(parseFloat(value1) > parseFloat(value2)){//如果当前值 大于 下一个值
                                        var temp = trsValue[j]; //那么就记住 大 的那个值
                                        trsValue[j] = trsValue[i];
                                        trsValue[i] = temp;
                                    }
                              //  }
                            }
                        }
                    }
                    for(var i = 0; i < len; i++){
                        //将排序完的 值 插入到 表格中
                        //:eq(index) 匹配一个给定索引值的元素
                        $("tbody tr:eq(" + i + ")").html(trsValue[i].split(".separator")[2]);
                        //console.log($("tbody tr:eq(" + i + ")").html())
                    }
                    sortIndex = index;
                }
                //IP转成整型 ？？？？？
                function ip2int(ip){
                    var num = 0;
                    ip = ip.split(".");
                    //Number() 函数把对象的值转换为数字。
                    num = Number(ip[0]) * 256 * 256 * 256 + Number(ip[1]) * 256 * 256 + Number(ip[2]) * 256 + Number(ip[3]);
                    return num;
                }
                })

