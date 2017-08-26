
$(document).ready(function(){ 
$('#button').click(function(){
    pedown = document.getElementById("pedown").value;
    peup = document.getElementById("peup").value;

    psdown=document.getElementById("psdown").value;
    psup=document.getElementById("psup").value;

    pbdown=document.getElementById("pbdown").value;
    pbup=document.getElementById("pbup").value;

    pcdown=document.getElementById("pcdown").value;
    pcup=document.getElementById("pcup").value;


    marketdown = document.getElementById("marketdown").value;
    marketup= document.getElementById("marketup").value;


    turnoverdown = document.getElementById("turnoverdown").value;
    turnoverup = document.getElementById("turnoverup").value;

    onedayup = document.getElementById("1dayup").value;
    onedaydown = document.getElementById("1daydown").value;

    fivedayup= document.getElementById("5dayup").value;
    fivedaydown= document.getElementById("5daydown").value;


    tendayup= document.getElementById("10dayup").value;
    tendaydown= document.getElementById("10daydown").value;



    thirtydayup= document.getElementById("30dayup").value;
    thirtydaydown= document.getElementById("30daydown").value;


    nightydayup= document.getElementById("90dayup").value;
    nightydaydown= document.getElementById("90daydown").value;





    var data={
        "peup":peup,
        "pedown":pedown,
        "psup":psup,
        "psdown":psdown,
        "pbdown":pbdown,
        "pbup":pbup,
        "pcdown":pcdown,
        "pcup":pcup,
        "onedayup":onedayup,
        "onedaydown":onedaydown,
        "fivedayup":fivedayup,
        "fivedaydown":fivedaydown,
        "tendayup":tendayup,
        "tendaydown":tendaydown,
        "thirtydayup":thirtydayup,
        "thirtydaydown":thirtydaydown,
        "nightydayup":nightydayup,
        "nightydaydown":nightydaydown,
        "marketdown":marketdown,
        "marketup":marketup,
        "turnoverdown":turnoverdown,
        "turnoverup":turnoverup

    };  
    $.ajax({ 
        type:"GET", 
        url:"/stocks/",
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