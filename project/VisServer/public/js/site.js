
/*  具体站点显示
 * 状态值采用订阅方式，服务器推送
 * 数据值在用户点击后获取
*/


var socket = io.connect('http://localhost:8089');

socket.on('site',function(data){
    console.log(data);
});
socket.on('queryResult',function(data){
    console.log(data);
    $("#test1").text(data);
});


// var data = [
//      {name: '0', value: 10},
//      {name: '1', value: 10},
//      {name: '2', value: 10},
//      {name: '3', value: 10},
//      {name: '4', value: 10},
//      {name: '5', value: 10},
//      {name: '6', value:10}
// ];
// var geoCoordMap = {
//     '0':[124.37,40.13],
//     '1':[120.836932,40.711052],
//     '2':[122.070714,41.119997],
//     '3':[121.62,38.92],
//     '4':[121.15,41.13],
//     '5':[123.73,41.3],
//     '6':[122.85,41.12]
// };
var geoCoordMap={};
$.ajaxSettings.async = false;  
$.get('/js/guizhougeoCoordMap.json').done(function (data){
    geoCoordMap=data;
});

$.ajaxSettings.async = true;

var chart=echarts.init($('#site')[0]);


var convertData = function () {
    var res = [];
    for (var i = 0; i < 100; i++) {
        var geoCoord = geoCoordMap[i];
        if (geoCoord) {
            res.push({
                name: i,
                value: geoCoord
            });
        }
    }
    return res;
};

var option = {
    backgroundColor: '#404a59',
    title: {
        text: '贵州省采集站点分布一览',
        subtext: '',
        sublink: '',
        left: 'center',
        textStyle: {
            color: '#fff'
        }
    },
    tooltip : {
        showContent:false,
    },
    legend: {
        orient: 'vertical',
        y: 'bottom',
        x:'right',
        data:['采集站点'],
        textStyle: {
            color: '#fff'
        }
    },
    geo: {
        map: '贵州',
        label: {
            emphasis: {
                show: false
            }
        },
        roam: true,
        itemStyle: {
            normal: {
                areaColor: '#323c48',
                borderColor: '#111'
            },
            emphasis: {
                areaColor: '#2a333d'
            }
        }
    },
    series : [
        {
            name: '采集站点',
            type: 'scatter',
            coordinateSystem: 'geo',
            data: '',
            symbolSize: function (val) {
                return val[2] / 10;
            },
            label: {
                normal: {
                    formatter: '{b}',
                    position: 'right',
                    show: false
                },
                emphasis: {
                    show: true
                }
            },
            itemStyle: {
                normal: {
                    color: '#ddb926'
                }
            }
        },
        {
            name: 'Top 5',
            type: 'effectScatter',
            coordinateSystem: 'geo',
            data: convertData(),
            symbolSize: function (val) {
                return val[2] / 10;
            },
            showEffectOn: 'render',
            rippleEffect: {
                brushType: 'stroke'
            },
            hoverAnimation: true,
            label: {
                normal: {
                    formatter: '{b}',
                    position: 'right',
                    show: true
                }
            },
            itemStyle: {
                normal: {
                    color: '#f4e925',
                    shadowBlur: 10,
                    shadowColor: '#333'
                }
            },
            zlevel: 1
        }
    ]
};

chart.setOption(option);
chart.on('click', function (params) {
    if (params.componentType === 'series') {
        socket.emit('query',params.name);
    }
});