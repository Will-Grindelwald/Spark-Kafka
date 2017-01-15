module.exports = function(app){	
	app.get('/',function(req,res){
	res.render('test');
});
app.use('/map',require('./map'));
};
