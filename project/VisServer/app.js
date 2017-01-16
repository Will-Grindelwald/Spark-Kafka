var path = require('path');
var express = require('express');
var routes = require('./routes');
var ejs = require('ejs');
var fs = require('fs');
var http = require('http');
var socketio = require('socket.io');

module.exports = function() {
    var app = express();
    var server = http.createServer(app);
    var io = socketio.listen(server);
    app.engine('html', ejs.__express);
    app.set('views', path.join(__dirname, 'views'));
    app.set('view engine', 'html');
    app.use(express.static(path.join(__dirname, 'public')));
    routes(app);
    require('./socketio')(io);
    return server;
}
