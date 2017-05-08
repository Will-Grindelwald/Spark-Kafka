var http = require("http");
var fs = require("fs");

http.createServer(function (req, res) {
  var index     = "./views/sensor_data.html";
  var jsIndex   = "./public/echarts.js";
  var fileName;
  var interval;
  
  if (req.url === "/")
    fileName = index;
  else
    fileName = "." + req.url;
  console.log(fileName);
  if (fileName === "./stream") {
    res.writeHead(200, {"Content-Type":"text/event-stream", "Cache-Control":"no-cache", "Connection":"keep-alive"});
    var redis = require('redis');
    var RDS_HOST = '192.168.125.171';
    var  RDS_PORT = 6379;
    client = redis.createClient(RDS_PORT,'192.168.125.171');
    client.on('ready',function(res){
    console.log('ready');
    });
 client.subscribe("messages");
 client.on("message", function (channel, message) {
    console.log("sub channel " + channel + ": " + message);
    var content = "data:" +message+ "\n\n";
    var b = res.write(content);
    if(!b)console.log("Data got queued in memory (content=" + content + ")");
    else console.log("Flushed! (content=" + content + ")");
 }); 
    req.connection.on("close", function(){
     client.unsubscribe();
    client.quit();
    res.end();
    //clearInterval(timer);
    console.log("Client closed connection. Aborting.");
    });
  }else if (fileName === jsIndex){
    fs.exists(fileName, function(exists) {
      if (exists) {
        fs.readFile(fileName, function(error, content) {
          if (error) {
            res.writeHead(500);
            res.end();
          } else {
            res.writeHead(200, {"Content-Type":"application/javascript"});
            res.end(content, "utf-8");
          }
        });
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  }else if (fileName === cssIndex ){
   fs.exists(fileName, function(exists) {
      if (exists) {
        fs.readFile(fileName, function(error, content) {
          if (error) {
            res.writeHead(500);
            res.end();
          } else {
            res.writeHead(200, {"Content-Type":"text/css"});
            res.end(content, "utf-8");
          }
        });
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  }else if (fileName === testIndex ){
   fs.exists(fileName, function(exists) {
      if (exists) {
        fs.readFile(fileName, function(error, content) {
          if (error) {
            res.writeHead(500);
            res.end();
          } else {
            res.writeHead(200, {"Content-Type":"text/css"});
            res.end(content, "utf-8");
          }
        });
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  }
  else if (fileName === index) {
    fs.exists(fileName, function(exists) {
      if (exists) {
        fs.readFile(fileName, function(error, content) {
          if (error) {
            res.writeHead(500);
            res.end();
          } else {
            res.writeHead(200, {"Content-Type":"text/html"});
            res.end(content, "utf-8");
          }
        });
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  } else {
    res.writeHead(404);
    res.end();
  }

}).listen(6679, "127.0.0.1");
console.log("Server running at http://127.0.0.1:6679/");