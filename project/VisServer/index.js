var path=require('path');
var express = require('express');
var app=express();
var routes=require('./routes');
var ejs = require('ejs'); 


app.engine('html', ejs.__express);
app.set('views',path.join(__dirname,'views'));
app.set('view engine', 'html');
app.use(express.static(path.join(__dirname,'public')));
routes(app);


app.listen(8089,function(){
	console.log("server is running on port 8089");
});
