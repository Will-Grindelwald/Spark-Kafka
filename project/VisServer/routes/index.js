module.exports = function(app){	
	app.get('/',function(req,res){
	res.render('map');
});
app.use('/map',require('./map'));
app.use('/site',require('./site'));
};
