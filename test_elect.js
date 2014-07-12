var Elect = require('.jigzelect'); 

var debug = require('debug')('testelect'); 

var conf = {
	zk_uri : "localhost:2181"
	, zk_timeout: 2000
	, zk_spindelay: 1000
	, zk_retries: 0 
//	, zk_path_elect: '/jigelect'
//	, cache_ttl : 0
//	, cache_checkperiod: 0
}

var elect = new Elect(conf, 5); 

for (var i=0; i< 3 ; i++) {
	var ctxname = 'ctx_'+i; 
	elect.addElectObject(ctxname, '/jigzones/jigcontexts/'+ctxname, 3, function(err, res) {
		if (err) console.log("add election object error: %s", err); 
		debug('added election object:%s', res); 
	}); 
}


setTimeout( function(){
   debug('reading election object...'); 
   for (var i=0; i< 3 ; i++) {
	var ctxn = 'ctx_'+i; 
 	elect.getElectObject(ctxn, function(error, data){
        	if (error) { debug('error reading object: %s', error); return ; }
                debug('read object => %j', data);
       });
   }
}, 1000); 

elect.anyCandidate(); 

function getLeads(){  
   elect.getMyLeaders(function(err, res){
	debug("Context I Lead are :"); 
	if (err ) { debug(err); return ; } 
	debug("I lead the following election object: %j", res); 
   });
}

function getNumberOfLeads(){
   elect.getNumberOfLeads(function(err,res){
	if (err) { debug(err); return ; } 
	debug('number of tenants I lead: %s', res);
  });
}

function revokeLead(key) {
    elect.revokeCandidate(key, function(err){
                if (err) debug('error revoking candidate %s', key);
    });
}

elect.on('connected', function(data){
	debug('connected to %s', data); 
}); 

elect.on('disconnected', function(data){
	debug('disconnected'); 
	debug(data); 
}); 

elect.on('leader-granted', function(data){
	debug("LEADER GRANT EVENT  :: I'm the leader for %j", data); 

	getLeads(); 

	getNumberOfLeads(); 

	debug("for some reason... I revoke my lead..."); 

//	revokeLead(data.key);

	// try to candidate, we shouldn't be allowed
//	elect.candidate(data.key); 

}); 

elect.on('leader-released', function(data){
	debug("LEADER RELEASED EVENT :: Release leader role for %j", data); 
	debug("for some reason, ... I want to try to candidate again ...."); 
	elect.candidate(data.key);  
});
