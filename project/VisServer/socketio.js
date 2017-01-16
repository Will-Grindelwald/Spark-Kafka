var Redis = require('ioredis');
var fs = require('fs');
// var nodes = [{
//     port: 6379,
//     host: '192.168.125.171'
// }, {
//     port: 7000,
//     host: '192.168.125.171'
// }];
// var cluster = new Redis.Cluster(nodes);
var redis=new Redis(6379, '192.168.125.171'); 

/*   订阅站点信息
 * 由Redis服务端推送
 * 推送消息有：站点丢失
 *
 */
//var sub = new Redis.Cluster(nodes);
var sub = new Redis(6379, '192.168.125.171'); 
sub.subscribe('site_error', 'test', function(err, count) {
    console.log(err);
});


//添加个测试
// cluster.set('本溪', 'tmper:25,humi:80%,status:95%');
// cluster.get('foo', function(err, res) {
//     console.log(res);
// });
var data = [];
var tmp = [];

fs.readFile('public/js/data.json', function(err, data) {
    if (err)
        throw err;
    tmp = JSON.parse(data);
});



module.exports = function(io) {
    io.on('connect', function(socket) {
        console.log('a user connected');
        sub.on('message', function(channel, message) {
            console.log(channel, message);
            var json = JSON.parse(message);
            for (var i = 0; i < tmp.length; i++) {
                if (tmp[i][1].name == json.name) {
                    tmp[i][1].status = json.status;
                    tmp[i][1].value = json.value;
                }
            }
            fs.writeFile('public/js/data.json', JSON.stringify(tmp, null, 4), function(err) {
                if (err) {
                    console.log(err);
                } else {
                    console.log("JSON saved to " + 'public/js/data.json');
                }
            });
            socket.emit('siteError', message);
        });

        //响应用户查询
        socket.on('query', function(data) {
            redis.get(data, function(err, res) {
                console.log(data + ":" + res);
                socket.emit('queryResult', data + ":" + res);

            });

        })
    });
};
