node-zookeeper-leader-election
==============================

This is a simple leader election based on NodeJs and node-zookeeper-client. 

v0.0.1 under test

DEBUG=zkelect,testelect node test_elect.js

create a new ZKElect object: 

	var ZkElect = require('zkelect'); 
	var elect = new ZkElect(); 


adding object element for which other node can candidate to: 

	elect.addElectObject(key, zoo_path, max, function(err, res){ ...} );

	elect.getElectObject(key, function(err,res){ ... } );  

	elect.delElectObject (key, function(err, res){ ... }); 

	elect.getElectObjects (function(err, res){ ... }); 


knowing when we get connected, disconnected 

	elect.on('connected', function(data){
	        debug('connected to %s', data); 
	}); 

	elect.on('disconnected', function(data){
	        debug('disconnected'); 
	        debug(data); 
	});


candidating to any elements, subject to a maximum number of leading roles

	elect.anyCandidate ();


knowing when we get the lead for an element 

	elect.on('leader-granted', function(data){
	        debug("LEADER GRANT EVENT  :: I'm the leader for %j", data); 
	}); 

	elect.on('leader-released', function(data){
        	debug("LEADER RELEASED EVENT :: Release leader role for %j", data); 
	});



other methods: 

	elect.candidate(key);
	elect.assessLeader(key, mypath);
	elect.revokeCandidate(key, function(err,res){...});
	elect.setMaxTenants(max); 
	elect.getMyLeaders(function (err,res){...});
	elect.getNumberOfLeads(function(err,res){...});


Further information will be included later
