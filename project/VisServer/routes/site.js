var express = require('express');
var router =express.Router();

router.get('/',function(req,res,next){
	res.setHeader('Access-Control-Allow-Origin', 'http://127.0.0.1:8089');
	res.render('site');
})

module.exports = router;