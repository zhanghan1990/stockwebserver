$(document).ready(function(){
$('#startbutton').click(function(){
    starttime = document.getElementById("starttime").value;
    endtime = document.getElementById("endtime").value;

    var editor = ace.edit("code");
    code= editor.getValue();


    var data={
        "starttime":starttime,
        "endtime":endtime,
        "code":code
    };  
    $.ajax({ 
        type:"GET", 
        url:"/startbacktest",
        data: data,
        dataType:"json", 
        success:function(data){
            var tabVar="";

            tabVar+="<table class=\"table\" id=\"choosestock\"><thead> <tr>"
            tabVar+="<th scope=\"row\">名称</th>"
            tabVar+="<th scope=\"row\">代码</th>"
            tabVar+="<th scope=\"row\",type=\"number\">涨跌幅(％)</th></tr></thead>"
            
            $.each(data,function(i,n){
                tabVar+="<tr>";
                tabVar+="<td><a href=\"/stockdetail/"+n["code"]+"-"+n["stockname"]+"\"><font>"+n["stockname"]+"</td>";   
                tabVar+="<td><a href=\"/stockdetail/"+n["code"]+"-"+n["stockname"]+"\"><font>"+n["code"]+"</td>";    
                if( parseFloat(n["change"])>0){
                    tabVar+="<td><font color=\"red\">"+n["change"]+"</td>";
                }else if(parseFloat(n["change"])<0){
                    tabVar+="<td><font color=\"green\">"+n["change"]+"</td>";
                }else{
                    tabVar+="<td>"+n["change"]+"</td>";
                }
               
                tabVar+="</tr>";
            
            });
        tabVar+="</table>";

        document.getElementById("detailTabShow").innerHTML=tabVar
        }
        }); 
        return false; 
            }); 
}); 